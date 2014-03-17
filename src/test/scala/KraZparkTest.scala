import org.scalatest._

import org.apache.spark.{SparkContext, SparkConf}
import SparkContext._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Seconds}

import com.codahale.metrics._
import com.codahale.metrics.jvm._
import java.util.concurrent.TimeUnit
import java.net.InetSocketAddress
import scala.concurrent.SyncVar

import org.apache.log4j.{Level, Logger}

// import com.esotericsoftware.kryo.Kryo
// import org.apache.spark.serializer.KryoRegistrator

/*class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Array[Int]])
  }
}*/

object Metrics2 {
  val metricRegistry = new MetricRegistry()

}

trait Instrumented2 extends nl.grons.metrics.scala.InstrumentedBuilder {
  val metricRegistry = Metrics.metricRegistry
  metricRegistry.register(MetricRegistry.name("jvm", "memory"), new MemoryUsageGaugeSet());
}

class KraZparkSpec extends FlatSpec with Matchers with Instrumented  with BeforeAndAfter {
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

  /*"Spark" should "spark" in {
    val data = 1 to 1000000

    val rdd = sc.parallelize(data, 2).persist(StorageLevel.MEMORY_ONLY_SER)

    for( i <- 1 to 100 ) {
      mzuring.time {
        assert(rdd.reduce( _ + _ ) == 1784293664)
      }
    }

  }*/

  import scalaz.concurrent.Task
  import scalaz.stream._
  import scala.concurrent.duration._

  def stdOutLines[I]: Sink[Task, I] =
    Process.constant{ (s: I) => Task.delay { println(s" ----> [${System.nanoTime}] *** $s") }}


  /** Generates continuous stream of positive naturals */
  def naturals: Process[Task, Int] = {
    def go(i: Int): Process[Task, Int] = Process.await(Task.delay(i)){ i => Process.emit(i) ++ go(i+1) }
    go(0)
  }

  def naturalsEvery(duration: Duration): Process[Task, Int] =
    (naturals zipWith Process.awakeEvery(duration)){ (i, b) => i }


  "KraZpark" should "crazy sparkle" in {
    import scalaz.stream._
    import scalaz.concurrent.Task

    import org.apache.spark.zpark._
    import KraZpark._

    implicit val sssc = ssc

    /** parallelize by batch of 4 and count by values */
    val p10 = Process(1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L).parallelize(4).countRDDByValue().continuize()
    val p1 = Process(1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L).parallelize(4).mapRDD(_ + 1L).reduceRDD(_ + _).continuize()

    /** generate naturals every 10 milliseconds and then parallelize by batch of 100 and count
      * and recontinuize the stream (keep only the first 100000s)
      */
    val p2 =
      naturalsEvery(10 milliseconds)
      .take(1000)
      .parallelize(100)
      .countRDD()
      .continuize()

    /** generate naturals every 10 milliseconds and takes the first 5000
      * and discretize the stream by window of 500ms
      * apply a distributed count
      * and recontinuize the stream
      */
    val p3 =
      naturalsEvery(10 milliseconds)
      .take(5000)
      .discretize(500 milliseconds)
      .countRDD()
      .continuize()

    /** generate naturals every 10 milliseconds and takes the first 500
      * discretize the stream by window of 500ms keeping track of time
      * redispatch RDD by window of 1000ms
      * apply a distributed count
      * and for each RDD, compute the count (not nice because blocking)
      */
    val p4 =
      naturalsEvery(10 milliseconds)
      .take(500)
      .discretizeKeepTime(500 milliseconds)
      .windowRDD(1000 milliseconds)
      .map { case (time, rdd) =>
        (time, rdd.count())
      }

    val p60 = naturalsEvery(100 milliseconds).take(50).discretize(250 milliseconds)
    val p61 = naturalsEvery(100 milliseconds).take(50).discretize(250 milliseconds)
    val p6 = (p60 zipWith p61)( (a,b) => new org.apache.spark.rdd.UnionRDD(ssc.sparkContext, Seq(a,b)) ).countRDDByValue().continuize()

    println("********************** P1 **************************")
    (p1 through stdOutLines).run.run

    println("********************** P2 **************************")
    (p2 through stdOutLines).run.run

    println("********************** P3 **************************")
    (p3 through stdOutLines).run.run

    println("********************** P4 **************************")
    (p4 through stdOutLines).run.run

    println("********************** P5 **************************")
    val p5 =
      io.linesR("testdata/fahrenheit.txt")
        .filter(s => !s.trim.isEmpty && !s.startsWith("//"))
        .map(line => line.toDouble)
        .discretize(100 milliseconds)
        .mapRDD { x => (x, 1L) }
        .groupByKey()
        .mapRDD { case (k, v) => (k, v.size) }
        .continuize()

    (p5 through stdOutLines).run.run

    println("********************** P6 **************************")
    (p6 through stdOutLines).run.run
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