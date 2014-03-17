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


class ZparkSpec_Part1 extends FlatSpec with Matchers with Instrumented  with BeforeAndAfter with ZparkTestUtils {
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
    ssc = new StreamingContext(clusterUrl, "SparkSerial", Seconds(1))

    // reporter = ConsoleReporter.forRegistry(metricRegistry)
    //                           .convertRatesTo(TimeUnit.SECONDS)
    //                           .convertDurationsTo(TimeUnit.MILLISECONDS)
    //                           .build();
    // reporter.start(1, TimeUnit.MINUTES);
  }

  //private[this] val mzuring = metrics.timer("mm")


  "Zpark" should "dstreamize a Process & count" in {
    import org.apache.spark.zpark._

    val p: Process[Task, Int] = naturalsEvery(50 milliseconds).take(100)

    val (sink, dstream) = dstreamize(p, ssc)

    dstream.count().print()

    ssc.start()

    sink.run.run

    ssc.awaitTermination(1000)
  }


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