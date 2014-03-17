/** I use spark package to hack all those private Spark classes */
package org.apache.spark
package zpark

import scala.reflect.ClassTag

import java.util.prefs.Preferences
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.storage.StorageLevel

import scalaz._
import Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

trait ZparkDStream {
  import Process._

  def receiver2Sink[T](receiver: ZparkReceiver[T]): Sink[Task, T] =
    Process.eval(
      Task.delay { (t:T) =>
        Task.delay {
          receiver.blockGenerator += t
        }
      }
    ).repeat

  def dstreamize[T : ClassTag](
    p: Process[Task, T],
    ssc: StreamingContext,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
  ): (Process[Task, Unit], ZparkInputDStream[T]) = {

    // Builds a custom LocalInputDStream
    val dstream = new ZparkInputDStream[T](ssc, storageLevel)

    // Builds a Sink pushing into dstream receiver
    val sink = receiver2Sink[T](dstream.receiver.asInstanceOf[ZparkReceiver[T]])

    // Finally pipes the process to the sink and when finished, closes the dstream
    val res =
      (p to sink)
      .append ( eval(Task.delay{}).evalMap { _ => Task.delay { dstream.stop(); } } )
      .handle { case e: Exception =>
        println("Stopping on error "+e.getMessage)
        e.printStackTrace()
        Process.eval(Task.delay{}).evalMap(_ => Task.delay { dstream.stop(); })
      }

    (res, dstream)
  }

}

class ZparkInputDStream[T : ClassTag](
  @transient ssc_ : StreamingContext,
  storageLevel: StorageLevel
) extends LocalInputDStream[T](ssc_)  {

  lazy val receiver = new ZparkReceiver[T](storageLevel)

}

class ZparkReceiver[T : ClassTag](
  storageLevel: StorageLevel
) extends LocalReceiver[T] {

  lazy val blockGenerator = new BlockGenerator(storageLevel)

  protected override def onStart() {
    blockGenerator.start()
    logInfo("Zpark Local Receiver started")
  }

  protected override def onStop() {
    blockGenerator.stop()
    logInfo("Zpark Local Receiver stopped")
  }

}
