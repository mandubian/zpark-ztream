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

class ZparkSpec_Part2 extends FlatSpec with Matchers with Instrumented  with BeforeAndAfter with ZparkTestUtils  {
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
    ssc = new StreamingContext(clusterUrl, "SparkStreamStuff", Seconds(1))

    // reporter = ConsoleReporter.forRegistry(metricRegistry)
    //                           .convertRatesTo(TimeUnit.SECONDS)
    //                           .convertDurationsTo(TimeUnit.MILLISECONDS)
    //                           .build();
    // reporter.start(1, TimeUnit.MINUTES);
  }

  //private[this] val mzuring = metrics.timer("mm")

  "Zpark" should "echo client data with echo server" in {
    // local bind address
    val addr = NioUtils.localAddress(12345)

    // the stop signal initialized to false
    val stop = async.signal[Boolean]
    stop.set(false).run

    // Create the server controlled by the previous signal
    val stoppableServer = (stop.discrete wye NioServer.echo(addr))(wye.interrupt)

    // Run server in async without taking care of output data
    stoppableServer.runLog.runAsync( _ => ())

    // Sleeps a bit to let server listen
    Thread.sleep(300)

    // create a client that sends 1024 random bytes
    val dataArray = Array.fill[Byte](1024)(1)
    scala.util.Random.nextBytes(dataArray)
    val clientOutput = NioClient.echo(addr, Bytes.of(dataArray))

    // Consume all received data in a blocking way...
    val result = clientOutput.runLog.run

    println("Client received:"+result)
    // stop server
    stop.set(true)
  }

/*
  "Zpark" should "send client data to nio dstreamized server" in {
    import org.apache.spark.zpark._

    // the bind address
    val addr = NioUtils.localAddress(11100)

    // the stop signal initialized to false
    val stop = async.signal[Boolean]
    stop.set(false).run

    // create the server controlled by the previous signal
    val stoppableServer = (stop.discrete wye NioServer.ackSize(addr))(wye.interrupt)

    // Create a client that sends a natural integer every 50ms as a string
    val clientData: Process[Task, Bytes] = naturalsEvery(50 milliseconds).take(100).map(i => Bytes.of(i.toString.getBytes))
    val clientOutput = NioClient.sendAndCheckSize(addr, clientData)

    // dstreamize the server in the streaming context
    val (consumer, dstream) = dstreamize(stoppableServer, ssc)

    // prepare dstream output
    dstream.map( bytes => new String(bytes.toArray) ).print()

    // start the streaming context
    ssc.start()

    // Run the server just for its effects
    consumer.run.runAsync( _ => () )

    // Sleeps a bit to let server listen
    Thread.sleep(300)

    // run the client
    clientOutput.runLog.run

    // Await SSC termination a bit
    ssc.awaitTermination(1000)

    // stop server
    stop.set(true)
  }
*/
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