/** I use spark package to hack all those private Spark classes */
package org.apache.spark
package zpark

import java.net.InetSocketAddress
import scalaz.{-\/, \/, \/-}

import scalaz.concurrent.{Task, Strategy}
import scalaz.stream._
import scala.concurrent.duration._
import ReceiveY._
import wye.Request

import java.util.concurrent.{ScheduledExecutorService, ExecutorService, ThreadFactory, Executors}
import java.nio.channels.spi.AsynchronousChannelProvider
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger


object NioServer {
  def apply(address: InetSocketAddress, w: Writer1[Bytes, Bytes, Bytes]): Process[Task, Bytes] = {
    val srv =
      nio.server(bind = address/*, reuseAddress = false*/)(CustomStrategy.buildAsynchronousChannelGroup).map { client =>
        client.flatMap {
          ex => ex.readThrough(w).run()
        }
      }

    val S = CustomStrategy.buildNewStrategy

    // Merges all client streams
    merge.mergeN(srv)(S)
  }

  def echo(address: InetSocketAddress): Process[Task, Bytes] = {
    // This is a Writer1 that echoes everything it receives to the client and emits it locally
    def echoAll: Writer1[Bytes, Bytes, Bytes] =
      Process.receive1[Bytes, Bytes \/ Bytes] { i =>
        // echoes on left, emits on right and then loop (fby = followed by)
        Process.emitSeq( Seq(\/-(i), -\/(i)) ) fby echoAll
      }

    apply(address, echoAll)

  }

  /** A server that acks all received packet by its size as a 4-bytes int */
  def ackSize(address: InetSocketAddress): Process[Task, Bytes] = {
    def ackAll: Writer1[Bytes, Bytes, Bytes] = {
      Process.receive1[Bytes, Bytes \/ Bytes] { i => 
        // println("server received " + new String(i.toArray))
        val arr = java.nio.ByteBuffer.allocate(4).putInt(i.size)
        arr.clear()
        //println("SZ:"+Bytes.of(arr))
        Process.emitSeq(Seq(\/-(i), -\/(Bytes.of(arr)))) fby ackAll
      }
    }

    apply(address, ackAll)
  }
}

object NioClient {
  import Process._

  /** a client sending all data in input process and awaiting them to be echo'ed by the server */
  def echo(address: InetSocketAddress, data: Bytes): Process[Task, Bytes] = {

    // the custom Wye managing business logic
    def echoLogic: WyeW[Bytes, Bytes, Bytes, Bytes] = {

      def go(collected: Int): WyeW[Bytes, Bytes, Bytes, Bytes] = {
        // Create a Wye that can receive on both sides
        receiveBoth {
          // Receive on left == receive from server
          case ReceiveL(rcvd) =>
            // `emitO` outputs on `I2` branch and then...
            emitO(rcvd) fby
              // if we have received everything sent, halt
              (if (collected + rcvd.size >= data.size) halt
              // else go on collecting
              else go(collected + rcvd.size))

          // Receive on right == receive on `W2` branch == your external data source
          case ReceiveR(data) => 
            // `emitW` outputs on `W` branch == sending to server
            // and loops
            emitW(data) fby go(collected)

          // When server closes
          case HaltL(rsn)     => Halt(rsn)
          // When client closes, we go on collecting echoes
          case HaltR(_)       => go(collected)
        }
      }

      // Init
      go(0)
    }

    // Finally wiring all...
    for {
      ex   <- nio.connect(address)
      rslt <- ex.wye(echoLogic).run(Process.emit(data))
    } yield {
      rslt
    }
  }

  /** a client that send all data emitted by input process and awaits for full size ack by server */
  def sendAndCheckSize(address: InetSocketAddress, data: Process[Task, Bytes]): Process[Task, Bytes] = {
    def ack: WyeW[Bytes, Int \/ Bytes, Bytes, Bytes] = {
      def go(buf: Bytes, collected: Int, expected: Int, collecting: Boolean): WyeW[Bytes, Int \/ Bytes, Bytes, Bytes] = {
        receiveBoth {
          case ReceiveL(-\/(_)) => go(buf, collected, expected, collecting)
          case ReceiveL(\/-(rcvd)) =>
            // println(s"Client received:$rcvd - Buf: $buf - Collected:$collected - Expected:$expected - collecting:$collecting")

            emitO(rcvd) fby {
              if(buf.size + rcvd.size < 4) {
                go(buf ++ rcvd, collected, expected, collecting)
              } else {

                // split Bytes 4-bytes per 4-bytes to Int, sum them & returns rest
                def splitter(buf: Bytes): (Int, Bytes) = {
                  var (l, r) = buf.splitAt(4)
                  var sz: Int = 0

                  while(!l.isEmpty) {
                    sz += l.asByteBuffer.getInt()

                    if(r.size >= 4) {
                      val (l_, r_) = r.splitAt(4)
                      l = l_
                      r = r_
                    }
                    else { l = Bytes.empty }
                  }
                  (sz, r.compact)
                }

                val (sz, nextBuf) = splitter(buf ++ rcvd)

                // println(s"BUF:$buf - NextBuf:${nextBuf} - SZ:$sz")
                if(collecting && collected + sz >= expected) halt
                else go(nextBuf, collected + sz, expected, collecting)
              }
            }
          case ReceiveR(data) =>
            // println("Client sending "+new String(data.toArray)+s" - Collected:$collected - Expected:$expected")
            tell(data) fby go(buf, collected, expected + data.size, collecting)
          case HaltL(rsn)     => /*println("server halt");*/ Halt(rsn)
          case HaltR(_)       => /*println("input halt");*/
            if(collected >= expected) halt
            else go(buf, collected, expected, true)
        }
      }

      go(Bytes.empty, 0, 0, false)
    }

    val AG = CustomStrategy.buildAsynchronousChannelGroup
    for {
      ex   <- nio.connect(to=address/*, noDelay=true, reuseAddress = false*/)(AG)
      rslt <- flow(ex, data)(ack)
      //rslt <- ex.wye(ack).run(p=data, terminateOn=Request.Both)
    } yield {
      rslt
    }
  }

  /** custom hacked flow function */
  def flow[I, W, I2, W2](self: Exchange[I, W], input: Process[Task, W2])(y: WyeW[W, Int \/ I, W2, I2])(implicit S: Strategy = Strategy.DefaultStrategy): Process[Task, I2] = {//Exchange[I2, W2] = {
    val wq = async.boundedQueue[W](0)
    val w2q = async.boundedQueue[W2](0)

    def mergeHaltBoth[I]: Wye[I,I,I] = {
      def go: Wye[I,I,I] =
        receiveBoth[I,I,I]({
          case ReceiveL(i) => /*println("L:"+i);*/ emit(i) fby go
          case ReceiveR(i) => /*println("R:"+i);*/ emit(i) fby go
          case HaltL(rsn) => /*println("HALTL:"+rsn);*/ Halt(rsn)
          case HaltR(rsn) => /*println("HALTR:"+rsn);*/ w2q.close.runAsync(_ => ()); go
        })
      go
    }

    def cleanup: Process[Task, Nothing] = eval_(wq.close) fby eval_(Task.delay(w2q.close.runAsync(_ => ())))
    def receive: Process[Task, I] = self.read onComplete cleanup
    def send: Process[Task, Unit] = wq.dequeue to self.write

    def sendAndReceive = {
      val (o, ny) = y.unemit
      (emitSeq(o) fby ((wq.size.discrete either receive).wye(w2q.dequeue)(ny)(S) onComplete cleanup) either send).flatMap {
        case \/-(o)       => halt
        case -\/(-\/(o))  => eval_(wq.enqueueOne(o))
        case -\/(\/-(b))  => emit(b)
      }
    }

    val res = Exchange(sendAndReceive, w2q.enqueue)

    res.read.wye((input to res.write).drain)(mergeHaltBoth[I2])
  }

}

object NioUtils {
  import scalaz._
  import Scalaz._
  import Process._

  def localAddress(port:Int) = new InetSocketAddress("127.0.0.1", port)

  def rechunk[I](p: I => (Vector[I], Boolean))(implicit I: Monoid[I]): Process1[I, I] = {
    import Process._

    def go(acc: I): Process1[I, I] = {
      await1[I].flatMap { i =>
        // println("i:"+i)
        val (v, emitLast) = p(i)

        // println(s"acc:$acc v:$v emitLast:$emitLast")
        v.size match {
          // imagine the separator is "\n"
          // "\n"
          case 0 => emit(acc) fby go(I.zero)
          // "1234"
          case 1 =>
            if(emitLast) { emit(I.append(acc, v.head)) fby go(I.zero) }
            else (go(I.append(acc, v.head)) orElse emit(I.append(acc, v.head)))
          case _ =>
            // \n1234
            if(v.head == I.zero) {
              if(emitLast) { emit(acc) ++ emitAll(v) fby go(I.zero) }
              else { emit(acc) ++ emitAll(v.init) ++ (go(v.last) orElse emit(v.last)) }
            }
            // 1234\n123\n
            else if(emitLast) {
              emit(I.append(acc, v.head)) ++ emitAll(v.tail) fby go(I.zero) }
            // 1234\n123...
            else { emit(I.append(acc, v.head)) ++ emitAll(v.init) ++ (go(v.last) orElse emit(v.last)) }
        }
      }
    }

    go(I.zero)
  }
}


object CustomStrategy {
  import Strategy._
  def SemiExecutorService: ExecutorService = {
    import Executors._
    newFixedThreadPool(Runtime.getRuntime.availableProcessors / 2,
    new ThreadFactory {
      def newThread(r: Runnable) = {
        val t = defaultThreadFactory.newThread(r)
        t.setDaemon(true)
        t
      }
    })
  }

  def buildNewStrategy: Strategy = Executor(SemiExecutorService)

  def buildAsynchronousChannelGroup = {
    val idx = new AtomicInteger(0)
    AsynchronousChannelProvider.provider().openAsynchronousChannelGroup(
      /*Runtime.getRuntime.availableProcessors() / 2*/ 2 max 2,
      new ThreadFactory {
        def newThread(r: Runnable): Thread = {
          val t = new Thread(r, s"scalaz-stream-nio-${idx.incrementAndGet()}")
          t.setDaemon(true)
          t
        }
      }
    )
  }

  def scheduler = {
    Executors.newScheduledThreadPool(4, new ThreadFactory {
      def newThread(r: Runnable) = {
        val t = Executors.defaultThreadFactory.newThread(r)
        t.setDaemon(true)
        t.setName("scheduled-task-thread")
        t
      }
    })
  }
}