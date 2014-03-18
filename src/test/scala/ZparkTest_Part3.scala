import org.scalatest._

import scala.concurrent.SyncVar

import java.util.concurrent.TimeUnit
import java.net.InetSocketAddress

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Seconds}
import SparkContext._

import com.codahale.metrics._
import com.codahale.metrics.jvm._

import scalaz.concurrent.Task
import scalaz.stream._
import scala.concurrent.duration._

import org.apache.log4j.{Level, Logger}

import org.apache.spark.zpark._
import scalaz.stream.async.mutable.Signal
import scalaz._
import Scalaz._

class ZparkSpec_Part3 extends FlatSpec with Matchers with Instrumented  with BeforeAndAfter with ZparkTestUtils  {
  var sc: SparkContext = null
  var ssc: StreamingContext = null
  var reporter: ConsoleReporter = null

  //val clusterUrl = "local-cluster[2,1,512]"
  val clusterUrl = "local[2]"

  before {
    // Make sure to set these properties *before* creating a SparkContext!
    // System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // System.setProperty("spark.kryo.registrator", "MyRegistrator")

    /*sc = new SparkContext(
      clusterUrl, "SparkSerial",
      System.getenv("SPARK_HOME") , SparkContext.jarOfClass(this.getClass)
    )*/

    //ssc = new StreamingContext(sc.getConf.set("spark.cleaner.ttl", "3600"), Seconds(1))
    Logger.getRootLogger.setLevel(Level.WARN)
    ssc = new StreamingContext(clusterUrl, "SparkStream", Seconds(1))

    // reporter = ConsoleReporter.forRegistry(metricRegistry)
    //                           .convertRatesTo(TimeUnit.SECONDS)
    //                           .convertDurationsTo(TimeUnit.MILLISECONDS)
    //                           .build();
    // reporter.start(1, TimeUnit.MINUTES);
  }

  //private[this] val mzuring = metrics.timer("mm")

  //////////////////////////////////////////////////
  // TRAINING CLIENT
  def trainingClient(addr: InetSocketAddress): Process[Task, Bytes] = {

    val basicTrainingData = Seq(
      "1,1,5.0",
      "1,2,1.0",
      "1,3,5.0",
      "1,4,1.0",
      "2,4,1.0",
      "2,2,5.0"
    )
    val trainingProcess = Process.emitAll(basicTrainingData map (s => Bytes.of((s+"\n").getBytes)))

    val client = NioClient.sendAndCheckSize(addr, trainingProcess)

    client
  }

  //////////////////////////////////////////////////
  // PREDICTION CLIENT
  def predictionClient(addr: InetSocketAddress): Process[Task, Bytes] = {
    //////////////////////////////////////////////////
    // PREDICTION DATA
    def rndData = 
      //(Math.abs(scala.util.Random.nextInt) % 4 + 1).toString +
      "1," +
      (Math.abs(scala.util.Random.nextInt) % 4 + 1).toString +
      "\n"

    val rndDataProcess = Process.eval(Task.delay{ rndData }).repeat

    val predictDataProcess =
      (Process.awakeEvery(/*100 milliseconds*/ (scala.util.Random.nextInt % 100 + 50) milliseconds) zipWith rndDataProcess){ (_, s) => Bytes.of(s.getBytes) }
        .take(100)

    val client = NioClient.sendAndCheckSize(addr, predictDataProcess)

    client
  }

  //////////////////////////////////////////////////
  // TRAINING SERVER
  def server(addr: InetSocketAddress): (Process[Task, Bytes], Signal[Boolean]) = {

    val stop = async.signal[Boolean]
    stop.set(false).run

    val server =
      ( stop.discrete wye NioServer.ackSize(addr) )(wye.interrupt)

    (server, stop)
  }

/*
  "Zpark" should "train a model with dstreamized process" in {
    import Process._

    import org.apache.spark.mllib.recommendation.ALS
    import org.apache.spark.mllib.recommendation.Rating
    import org.apache.spark.streaming.{Time, Milliseconds}
    import org.apache.spark.rdd.UnionRDD

    val addr = NioUtils.localAddress(11100)
    //////////////////////////////////////////////////
    // TRAINING SERVER
    val (trainingServer, trainingStop) = server(addr)

    //////////////////////////////////////////////////
    // TRAINING CLIENT
    val tclient = trainingClient(addr)

    //////////////////////////////////////////////////
    // DStreamize server
    val (trainingServerSink, trainingDstream) = dstreamize(
      trainingServer
          // converts bytes to String (doesn't care about encoding, it shall be UTF8)
          .map  ( bytes => new String(bytes.toArray) )
          // rechunk received strings based on a separator \n
          .pipe (NioUtils.rechunk { s:String => (s.split("\n").toVector, s.last == '\n') } )
      , ssc
    )

    //////////////////////////////////////////////////
    // Prepare dstream output
    trainingDstream.print()

    //////////////////////////////////////////////////
    // RUN
    val before = new Time(System.currentTimeMillis)

    // Start SSC
    ssc.start()

    // Launch server
    trainingServerSink.run.runAsync( _ => () )

    // Sleeps a bit to let server listen
    Thread.sleep(300)

    // Launch client
    tclient.run.run

    // Await SSC termination a bit
    ssc.awaitTermination(1000)
    // Stop server
    trainingStop.set(true).run
    val after = new Time(System.currentTimeMillis)

    // Build model
    val rdds = trainingDstream.slice(before.floor(Milliseconds(1000)), after.floor(Milliseconds(1000)))
    val union: RDD[String] = new UnionRDD(ssc.sparkContext, rdds)

    val ratings = union map { e =>
      e.split(',') match {
        case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)
      }
    }

    val model = ALS.train(ratings, 1, 20, 0.01)

    // Predict
    println("Prediction(1,3)=" + model.predict(1, 3))

  }
*/

  "Zpark" should "train a model with server1 & predict through server2 with one single client" in {
    import Process._

    import org.apache.spark.mllib.recommendation.ALS
    import org.apache.spark.mllib.recommendation.Rating
    import org.apache.spark.streaming.{Time, Milliseconds}
    import org.apache.spark.rdd.UnionRDD

    //////////////////////////////////////////////////
    // TRAINING
    val trainingAddr = NioUtils.localAddress(11100)

    // TRAINING SERVER
    val (trainingServer, trainingStop) = server(trainingAddr)

    // TRAINING CLIENT
    val tclient = trainingClient(trainingAddr)

    // DStreamize server
    val (trainingServerSink, trainingDstream) = dstreamize(
      trainingServer
          // converts bytes to String (doesn't care about encoding, it shall be UTF8)
          .map  ( bytes => new String(bytes.toArray) )
          // rechunk received strings based on a separator \n
          .pipe (NioUtils.rechunk { s:String => (s.split("\n").toVector, s.last == '\n') } )
      , ssc
    )

    // Prepare dstream output
    trainingDstream.print()

    //////////////////////////////////////////////////
    // PREDICTING
    val predictAddr = NioUtils.localAddress(11101)

    // PREDICTION SERVER
    val (predictServer, predictStop) = server(predictAddr)

    // PREDICTION CLIENT
    val pClient = predictionClient(predictAddr)

    // DStreamize server
    val (predictServerSink, predictDstream) = dstreamize(
      predictServer
          // converts bytes to String (doesn't care about encoding, it shall be UTF8)
          .map  ( bytes => new String(bytes.toArray) )
          // rechunk received strings based on a separator \n
          .pipe ( NioUtils.rechunk { s:String => /*println("SERVER "+s);*/ (s.split("\n").toVector, s.last == '\n') } )
      , ssc
    )

    // pipe dstreamed RDD to prediction model
    // and print result
    var model = new SyncVar[org.apache.spark.mllib.recommendation.MatrixFactorizationModel]

    predictDstream.map { s => 
      //println("RECV "+s)
      s.split(',') match {
        case Array(user, item) => (user.toInt, item.toInt)
      }
    }.transform { rdd =>
      model.get.predict(rdd)
    }.print()

    //////////////////////////////////////////////////
    // RUN
    val before = new Time(System.currentTimeMillis)

    // Start SSC
    ssc.start()

    // Launch server
    trainingServerSink.run.runAsync( _ => () )

    // Sleeps a bit to let server listen
    Thread.sleep(300)

    // Launch client
    tclient.run.run

    // Await SSC termination a bit
    ssc.awaitTermination(1000)
    // Stop server
    trainingStop.set(true).run
    val after = new Time(System.currentTimeMillis)

    val rdds = trainingDstream.slice(before.floor(Milliseconds(1000)), after.floor(Milliseconds(1000)))
    val union: RDD[String] = new UnionRDD(ssc.sparkContext, rdds)

    val ratings = union map {
      _.split(',') match {
        case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)
      }
    }

    model.set(ALS.train(ratings, 1, 20, 0.01))

    println("**** Model Trained -> Prediction(1,3)=" + model.get.predict(1, 3))

    // Launch server
    predictServerSink.run.runAsync( _ => () )

    // Sleeps a bit to let server listen
    Thread.sleep(300)

    // Launch client
    pClient.run.run

    // Await SSC termination a bit
    ssc.awaitTermination(1000)
    // Stop server
    predictStop.set(true).run

  }


/*
  "Zpark" should "train a model with server1 & predict through server2 with multiple clients" in {
    import Process._

    import org.apache.spark.mllib.recommendation.ALS
    import org.apache.spark.mllib.recommendation.Rating
    import org.apache.spark.streaming.{Time, Milliseconds}
    import org.apache.spark.rdd.UnionRDD

    //////////////////////////////////////////////////
    // TRAINING
    val trainingAddr = NioUtils.localAddress(11100)

    // TRAINING SERVER
    val (trainingServer, trainingStop) = server(trainingAddr)

    // TRAINING CLIENT
    val tclient = trainingClient(trainingAddr)

    // DStreamize server
    val (trainingServerSink, trainingDstream) = dstreamize(
      trainingServer
          // converts bytes to String (doesn't care about encoding, it shall be UTF8)
          .map  ( bytes => new String(bytes.toArray) )
          // rechunk received strings based on a separator \n
          .pipe (NioUtils.rechunk { s:String => (s.split("\n").toVector, s.last == '\n') } )
      , ssc
    )

    // Prepare dstream output
    trainingDstream.print()


    //////////////////////////////////////////////////
    // PREDICTING
    val predictAddr = NioUtils.localAddress(11101)

    // PREDICTION SERVER
    val (predictServer, predictStop) = server(predictAddr)

    // PREDICTION CLIENT
    val count = 10
    val pClients =
      scalaz.stream.merge.mergeN(Process.range(0, count).map(_ => predictionClient(predictAddr)))

    // DStreamize server
    val (predictServerSink, predictDstream) = dstreamize(
      predictServer
          // converts bytes to String (doesn't care about encoding, it shall be UTF8)
          .map  ( bytes => new String(bytes.toArray) )
          // rechunk received strings based on a separator \n
          .pipe ( NioUtils.rechunk { s:String => /*println("SERVER "+s);*/ (s.split("\n").toVector, s.last == '\n') } )
      , ssc
    )

    // THE HORRIBLE SYNCVAR CLUDGE (could have used atomic but not better IMHO)
    var model = new SyncVar[org.apache.spark.mllib.recommendation.MatrixFactorizationModel]
    // THE HORRIBLE SYNCVAR CLUDGE (could have used atomic but not better IMHO)

    predictDstream.map { s => 
      //println("RECV "+s)
      s.split(',') match {
        case Array(user, item) => (user.toInt, item.toInt)
      }
    }.transform { rdd =>
      model.get.predict(rdd)
    }.print()

    //////////////////////////////////////////////////
    // RUN
    val before = new Time(System.currentTimeMillis)

    // Start SSC
    ssc.start()

    // Launch server
    trainingServerSink.run.runAsync( _ => () )

    // Sleeps a bit to let server listen
    Thread.sleep(300)

    // Launch client
    tclient.run.run

    // Await SSC termination a bit
    ssc.awaitTermination(1000)
    // Stop server
    trainingStop.set(true).run
    val after = new Time(System.currentTimeMillis)

    val rdds = trainingDstream.slice(before.floor(Milliseconds(1000)), after.floor(Milliseconds(1000)))
    val union: RDD[String] = new UnionRDD(ssc.sparkContext, rdds)

    val ratings = union map {
      _.split(',') match {
        case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)
      }
    }

    model.set(ALS.train(ratings, 1, 20, 0.01))

    println("**** Model Trained -> Prediction(1,3)=" + model.get.predict(1, 3))

    // wait a bit to let everything finish
    Thread.sleep(2000)

    // Launch prediction server
    predictServerSink.run.runAsync( _ => () )

    // Sleeps a bit to let server listen
    Thread.sleep(300)

    // Launch prediction client
    pClients.run.run

    // Await SSC termination a bit
    ssc.awaitTermination(1000)
    // Stop server
    predictStop.set(true).run

  }
*/



// DEPRECATED CODE
  /*"Zpark" should "sparkle ML training" in {
    import scalaz._
    import Scalaz._
    import Process._
    import org.apache.spark.zpark._
    import org.apache.spark.mllib.recommendation.ALS
    import org.apache.spark.mllib.recommendation.Rating
    import org.apache.spark.streaming.Time
    import org.apache.spark.rdd.UnionRDD

    val stop = async.signal[Boolean]
    stop.set(false).run

    val trainingLocal = NioUtils.localAddress(11100)

    val trainingDataRecv = new SyncVar[Vector[String]]

    val trainingServer =
      ( /*(Process.sleep(2 seconds) ++ emit(true))*/ stop.discrete wye NioServer.ack(trainingLocal) )(wye.interrupt) map { bytes =>
        new String(bytes.toArray)
      } pipe NioUtils.rechunk { s:String => (s.split("\n").toVector, s.last == '\n') }

    val trainingData = Seq(
      "1,1,5.0",
      "1,2,1.0",
      "1,3,5.0",
      "1,4,1.0",
      "2,4,1.0",
      "2,2,5.0"
    )
    val trainingProcess = Process.emitAll(trainingData.map(_+"\n")).map { s => Bytes.of(s.getBytes) }

    val trainingClient = NioClient.send(trainingLocal, trainingProcess)

    trainingServer.runLog.runAsync {
      case -\/(e) => throw e
      case \/-(td) => trainingDataRecv.put(td.toVector)
    }

    Thread.sleep(300)

    trainingClient.runLog.timed(3000).run //.map(_.toSeq).flatten

    stop.set(true).run

    val recvData = trainingDataRecv.get(5000)

    println("RECVDATA:"+recvData.get)

    val ratings = ssc.sparkContext.parallelize(recvData.get).map { e =>
      //println("EE:"+e)
      e.split(',') match {
        case Array(user, item, rate) =>
          val r = Rating(user.toInt, item.toInt, rate.toDouble)
          //println("rating:"+r)
          r
      }
    }

    val numIterations = 20
    val model = ALS.train(ratings, 1, numIterations, 0.01)

    val predictLocal = NioUtils.localAddress(11101)

    val stop2 = async.signal[Boolean]
    stop2.set(false).run

    val server =
      ( stop2.discrete wye NioServer.ack(predictLocal) )(wye.interrupt) map { bytes =>
        new String(bytes.toArray)
      } pipe NioUtils.rechunk { s:String => (s.split("\n").toVector, s.last == '\n') }

    val data = Vector(
      "1,1",
      "1,2",
      "1,3",
      "1,4",
      "2,4",
      "2,2"
    )

    def rnd = 
      //(Math.abs(scala.util.Random.nextInt) % 4 + 1).toString +
      "1," +
      (Math.abs(scala.util.Random.nextInt) % 4 + 1).toString +
      "\n"

    val rndData = Process.eval(Task.delay{ rnd }).repeat

    val dataProcess =
      /*(rndData zipWith
        Process.awakeEvery(100 milliseconds).forwardFill
      ){ (c, i) => c }*/
      (Process.awakeEvery(10 milliseconds) zipWith
        rndData
      ){ (c, i) => i }
        .take(1000)
        .map { s => Bytes.of(s.getBytes) }
      //(Process.emitAll(data.map(_+"\n")) zipWith Process.awakeEvery(200 milliseconds)){ (i, b) => i }
      //Process.emitAll(data.map(_+"\n"))
      //rndData
      // .repeat

    val (sinked, dstream) = dstreamize(server, ssc)

    dstream map { _.split(',') match {
      case Array(user, item) => (user.toInt, item.toInt)
    }} transform { rdd =>
      model.predict(rdd)
    } print()

    ssc.start()

    val recv = new SyncVar[String]
    //val recv = new SyncVar[scalaz.\/[Throwable,scala.collection.immutable.IndexedSeq[String]]]

    sinked.run.runAsync { _ =>
      println("finished")
      recv.put("finished")
    }

    /*server.runLog.runAsync { e =>
      println("e:"+e)
      recv.put(e)
    }*/

    Thread.sleep(300)

    val client = NioClient.send(predictLocal, dataProcess)
    val count = 1
    val clients =
      scalaz.stream.merge.mergeN(Process.range(0,count).map(_ => client)).bufferAll

    clients.runLog/*.timed(15000)*/.run

    ssc.awaitTermination(5000)
    stop2.set(true).run

    val recvData2 = recv.get(10000)

    println("RECV:"+recvData2.get)
  }*/

  //it should "sparkle2" in {


    // TRAIN
    // val trainingData = ssc.sparkContext.textFile("testdata/test.data")
    // val ratings = trainingData.map { e =>
    //   println("EE:"+e)
    //   e.split(',') match {
    //     case Array(user, item, rate) =>
    //       val r = Rating(user.toInt, item.toInt, rate.toDouble)
    //       println("rating:"+r)
    //       r
    //   }
    // }

    // val numIterations = 20
    // val model = ALS.train(ratings, 1, numIterations, 0.01)

    // val usersProducts = ratings.map{ case Rating(user, product, rate)  => (user, product)}
    // val predictions = model.predict(usersProducts).map{
    //   case Rating(user, product, rate) => ((user, product), rate)
    // }

    // predictions foreach println

    // def localAddress(port:Int) = new InetSocketAddress("127.0.0.1", port)

    // val local = localAddress(11100)
    // val size: Int = 500
    // val array1 = Array.fill[Byte](size)(1)

    // val stop = async.signal[Boolean]
    // stop.set(false).run

    // val serverGot = new SyncVar[Throwable \/ IndexedSeq[String]]

    // val trainingServer =
    //   (NioServer.ack(local)
    //   .map { bytes => new String(bytes.toArray) } |>
    //   NioUtils.rechunk { s:String => (s.split("\n").toVector, s.last == '\n') })

    // val controlledServer = stop.discrete.wye(server)(wye.interrupt)

    //     //.runLog
    //     //.map (_.map(_.toSeq).flatten)
    //     //.runAsync(serverGot.put)

    // val data = Seq(
    //   "1,1,5.0",
    //   "1,2,1.0",
    //   "1,3,5.0",
    //   "1,4,1.0"
    // )
    // val dataProcess = Process.emitAll(data.map(_+"\n")).map { s => Bytes.of(s.getBytes) }

    //val (sinked, dstream) = dstreamize(controlledServer, ssc)

        //.count()
    //dstream.print()

    //ssc.start()

    //val before = new Time(System.currentTimeMillis)

    // sinked.run.runAsync { _ =>
    //   println("finished")
    //   serverGot.put(\/-(IndexedSeq("finished")));

    // }

    //Thread.sleep(300)

    // val clientGot =
    //   NioClient.send(local, dataProcess)
    //            .runLog.timed(3000)
    //            .run.map(_.toSeq).flatten

    // stop.set(true).run

    // println("FINAL:"+serverGot.get(5000))

    // val after = new Time(System.currentTimeMillis)
    // val rdds = dstream.slice(before, after)
    // val union: RDD[String] = new UnionRDD(ssc.sparkContext, rdds)


    //ssc.awaitTermination(2000)
    //serverGot.get(5000) should equal (Some(\/-(data)))
  //}

  /*"Zpark" should "sparkle4" in {
    import scalaz._
    import Scalaz._
    import org.apache.spark.zpark.{NioServer, NioClient, NioUtils}

    val stop = async.signal[Boolean]
    stop.set(false).run

    val trainingLocal = NioUtils.localAddress(11100)

    val trainingDataRecv = new SyncVar[Vector[String]]

    val trainingServer =
      ( /*(Process.sleep(2 seconds) ++ emit(true))*/ 
      stop.discrete wye NioServer.ack(trainingLocal) )(wye.interrupt) map { bytes =>
        new String(bytes.toArray)
      } //pipe NioUtils.rechunk { s:String => (s.split("\n").toVector, s.last == '\n') }

    val trainingData = Seq(
      "1,1,5.0",
      "1,2,1.0",
      "1,3,5.0",
      "1,4,1.0",
      "2,4,1.0",
      "2,2,5.0"
    )
    val trainingProcess = Process.emitAll(trainingData.map(_+"\n")).map { s => Bytes.of(s.getBytes) }

    val trainingClient = NioClient.send(trainingLocal, trainingProcess)

    trainingServer.runLog.runAsync {
      case -\/(e) => println("received exception:"+e.getMessage); throw e
      case \/-(td) => println("PUT"); trainingDataRecv.put(td.toVector)
    }

    Thread.sleep(300)

    trainingClient.runLog.timed(3000).run //.map(_.toSeq).flatten

    stop.set(true).run

    println("RECVDATA:"+trainingDataRecv.get(5000))

    Thread.sleep(2000)

    val predictLocal = NioUtils.localAddress(11101)

    val stop2 = async.signal[Boolean]
    stop2.set(false).run

    val server =
      ( stop2.discrete wye NioServer.ack(predictLocal) )(wye.interrupt) map { bytes =>
        val s =new String(bytes.toArray)
        println("RECV:"+s)
        s
      } //pipe NioUtils.rechunk { s:String => (s.split("\n").toVector, s.last == '\n') }

    val data = Vector(
      "1,1",
      "1,2",
      "1,3",
      "1,4",
      "2,4",
      "2,2",
      "2.1",
      "2.3"
    )

    val recv = new SyncVar[scalaz.\/[Throwable,scala.collection.immutable.IndexedSeq[String]]]

    val dataProcess =
      (Process.emitAll(data.map(_+"\n")).map { s => Bytes.of(s.getBytes) } zipWith
      Process.every(100 milliseconds)){ (c, i) => c }

    val client = NioClient.send(predictLocal, dataProcess)

    server.runLog.runAsync { e =>
      println("e:"+e)
      recv.put(e)
    }

    Thread.sleep(300)
    client.runLog.timed(5000).run

    stop2.set(true).run

    val recvData2 = recv.get(5000)

    println("RECV2:"+recvData2.get)
  }*/

  after {
    //reporter.report()
    //ssc.awaitTermination()
    ssc.stop()
    //sc.stop()
    ssc = null
    //sc = null
    System.clearProperty("spark.driver.port")
    //reporter.stop()
  }

}