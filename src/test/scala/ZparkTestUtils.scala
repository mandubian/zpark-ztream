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

// import com.esotericsoftware.kryo.Kryo
// import org.apache.spark.serializer.KryoRegistrator

/*class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Array[Int]])
  }
}*/

object Metrics {
  val metricRegistry = new MetricRegistry()

}

trait Instrumented extends nl.grons.metrics.scala.InstrumentedBuilder {
  val metricRegistry = Metrics.metricRegistry
  metricRegistry.register(MetricRegistry.name("jvm", "memory"), new MemoryUsageGaugeSet());
}


trait ZparkTestUtils {
  def stdOutLines[I]: Sink[Task, I] =
    Process.constant{ (s: I) => Task.delay { println(s" ----> [${System.nanoTime}] *** $s") }}

  /** Generates continuous stream of positive naturals */
  def naturals: Process[Task, Int] = {
    def go(i: Int): Process[Task, Int] = Process.await(Task.delay(i)){ i => Process.emit(i) ++ go(i+1) }
    go(0)
  }

  def naturalsEvery(duration: Duration): Process[Task, Int] =
    (naturals zipWith Process.awakeEvery(duration)){ (i, b) => i }
}
