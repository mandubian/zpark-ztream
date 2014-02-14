/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.zpark

import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.streaming._

import scala.reflect.ClassTag
import scalaz.stream._

import scalaz.concurrent.Task
import scalaz.{\/, -\/, \/-}
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.util.{Try, Success, Failure}

import ExecutionContext.Implicits.global

object Zpark {

  implicit def taskProcess2RDDProcess[I : ClassTag](p: Process[Task, I])(implicit scc: StreamingContext) =
    new TaskProcess2RDDProcess(p)

  implicit def taskRDDProcess2TaskProcess[I : ClassTag](p: Process[Task, RDD[I]])(implicit scc: StreamingContext) =
    new TaskRDDProcess2TaskProcess(p)

  implicit def extendProcessTimeRDD[I : ClassTag](p: Process[Task, (Long, RDD[I])])(implicit scc: StreamingContext) =
    new ExtendProcessTimeRDD(p)



  def scalaFuture2scalazTask[T](fut: => Future[T])(implicit ctx: ExecutionContext): Task[T] = {
    Task.async {
      register =>
        fut.onComplete {
          case Success(v) => register(\/-(v))
          case Failure(ex) => register(-\/(ex))
        }
    }
  }

  implicit def processRDDPairFunctions[F[_], K : ClassTag, V : ClassTag](p: Process[F, RDD[(K, V)]])(implicit scc: StreamingContext) =
    new ExtendPairProcessRDD(p)

  implicit def processRDDFunctions[F[_], K : ClassTag](p: Process[F, RDD[K]])(implicit scc: StreamingContext) =
    new ExtendProcessRDD(p)

  /**
   * A continuous stream of the time, computed using `System.nanoTime`.
   * Note that the actual granularity of these times depends on the OS, for instance
   * the OS may only update the current time every ten milliseconds or so.
   */
   def timeStream: Process[Task, Long] = Process.suspend {
    Process.repeatEval { Task.delay { System.nanoTime }}
  }

  /** A Wye that accumulates Right as long as Left Duration hasn't reach duration 
    * or accumuled maxSize and then emits a vector and reset accumulator
    */
  def tickeredQueue[I](duration: Duration, maxSize: Int = Int.MaxValue): Wye[Duration, I, Vector[I]] = {
    import scalaz.stream.ReceiveY.{ReceiveL, ReceiveR, HaltOne}

    def go(acc: Vector[I], last: Duration): Wye[Duration,I,Vector[I]] =
      Process.awaitBoth[Duration, I].flatMap {
        case ReceiveL(current) =>
          if(current - last > duration || acc.size >= maxSize)
            Process.emit(acc) ++ go(Vector(), current)
          else go(acc, last)
        case ReceiveR(i) => 
          if(acc.size + 1 >= maxSize) Process.emit(acc :+ i) ++ go(Vector(), last)
          else go(acc :+ i, last)
        case HaltOne(e) => 
          if(!acc.isEmpty) Process.emit(acc) ++ Process.Halt(e)
          else Process.Halt(e)
      }
    Process.awaitL[Duration].flatMap { firstDuration => go(Vector(), firstDuration) }
  }

  /** A Wye that accumulates Right as long as Left Duration hasn't reach duration 
    * or accumuled maxSize and then emits a vector and reset accumulator
    * it outputs the moment of emission
    */
  def tickeredQueueWithTime[I](duration: Duration, maxSize: Int = Int.MaxValue): Wye[Long, I, (Long, Vector[I])] = {
    import scalaz.stream.ReceiveY.{ReceiveL, ReceiveR, HaltOne}
    val nanos = duration.toNanos

    def go(acc: Vector[I], last: Long): Wye[Long, I, (Long, Vector[I])] =
      Process.awaitBoth[Long, I].flatMap {
        case ReceiveL(current) =>
          if(current - last > nanos || acc.size >= maxSize)
            Process.emit((current, acc)) ++ go(Vector(), current)
          else go(acc, last)
        case ReceiveR(i) =>
          if(acc.size + 1 >= maxSize) Process.emit((System.nanoTime, acc :+ i)) ++ go(Vector(), last)
          else go(acc :+ i, last)
        case HaltOne(e) => 
          if(!acc.isEmpty) Process.emit((System.nanoTime, acc)) ++ Process.Halt(e)
          else Process.Halt(e)
      }

    go(Vector(), System.nanoTime)
  }

  def defaultPartitioner(implicit ssc: StreamingContext) = {
    new HashPartitioner(ssc.sparkContext.defaultParallelism)
  }

  def defaultPartitioner(numPartitions: Int) = {
    new HashPartitioner(numPartitions)
  }

}

class TaskProcess2RDDProcess[I : ClassTag](val p: Process[Task, I])(implicit ssc: StreamingContext) {

  def parallelize(batchSize: Int): Process[Task, RDD[I]] =
    p.chunk(batchSize).filter(!_.isEmpty).evalMap { vec =>
      Task.delay{ ssc.sparkContext.parallelize(vec.toSeq) }
    }

  def discretize(batchDuration: Duration): Process[Task, RDD[I]] =
    (Process.duration wye p)(Zpark.tickeredQueue(batchDuration)).filter(!_.isEmpty).evalMap { vec =>
      Task.delay{ ssc.sparkContext.parallelize(vec.toSeq) }
    }

  def discretizeKeepTime(batchDuration: Duration): Process[Task, (Long, RDD[I])] =
    (Zpark.timeStream wye p)(Zpark.tickeredQueueWithTime(batchDuration)).filter(!_._2.isEmpty).evalMap { case (time, vec) =>
      Task.delay{ (time, ssc.sparkContext.parallelize(vec.toSeq)) }
    }

}

class TaskRDDProcess2TaskProcess[I : ClassTag](val p: Process[Task, RDD[I]])(implicit ssc: StreamingContext) {
  import org.apache.spark.SparkContext._

  def continuize(): Process[Task, I] =
    p.flatMap { rdd: RDD[I] =>
      val f = RemoteJobClosures.submitJobCollect(rdd)
      Process.await(
          Zpark.scalaFuture2scalazTask(rdd.collectAsync())
      )( seq => Process.emitSeq(seq) )
    }

}

object RemoteJobClosures {
  def submitJobCollect[I : ClassTag](rdd: RDD[I])(implicit ssc: StreamingContext): Future[Seq[I]] = {
    val results = new Array[Array[I]](rdd.partitions.size)

    ssc.sparkContext.submitJob[I, Array[I], Seq[I]](
      rdd,
      it => it.toArray,
      Range(0, rdd.partitions.size),
      //{ (index, arr) => arr.foreach(queue.enqueue(_)) },
      //process
      (index, data) => results(index) = data, 
      results.flatten.toSeq
    )
  }

}



class ExtendPairProcessRDD[F[_], K : ClassTag, V : ClassTag](val self: Process[F, RDD[(K, V)]])(implicit ssc: StreamingContext) {
  import SparkContext._
  import org.apache.spark.{Partitioner, HashPartitioner}
  import org.apache.spark.util.ClosureCleaner

  import Zpark._

  def reduceByKey(reduceFunc: (V, V) => V): Process[F, RDD[(K, V)]] = {
    reduceByKey(reduceFunc, defaultPartitioner)
  }

  def reduceByKey(reduceFunc: (V, V) => V, numPartitions: Int): Process[F, RDD[(K, V)]] = {
    reduceByKey(reduceFunc, defaultPartitioner(numPartitions))
  }

  def reduceByKey(reduceFunc: (V, V) => V, partitioner: Partitioner): Process[F, RDD[(K, V)]] = {
    val cleanedReduceFunc = ssc.sparkContext.clean(reduceFunc)
    combineByKey((v: V) => v, cleanedReduceFunc, cleanedReduceFunc, partitioner)
  }

  def groupByKey(): Process[F, RDD[(K, Vector[V])]] = {
    groupByKey(defaultPartitioner)
  }

  def groupByKey(numPartitions: Int): Process[F, RDD[(K, Vector[V])]] = {
    groupByKey(defaultPartitioner(numPartitions))
  }

  def groupByKey(partitioner: Partitioner): Process[F, RDD[(K, Vector[V])]] = {
    val createCombiner = (v: V) => Vector[V](v)
    val mergeValue = (c: Vector[V], v: V) => (c :+ v)
    val mergeCombiner = (c1: Vector[V], c2: Vector[V]) => (c1 ++ c2)
    combineByKey[Vector[V]](createCombiner, mergeValue, mergeCombiner, partitioner)
  }

  def combineByKey[C: ClassTag](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    partitioner: Partitioner,
    mapSideCombine: Boolean = true): Process[F, RDD[(K, C)]] = {

    self.map { rdd =>
      rdd.combineByKey[C](
        createCombiner, mergeValue, mergeCombiner, partitioner, mapSideCombine
      )
    }
  }
}


class ExtendProcessRDD[F[_], T : ClassTag](val process: Process[F, RDD[T]])(implicit ssc: StreamingContext) {
  import Zpark._
  import SparkContext._

  def mapRDD[U: ClassTag](f: T => U): Process[F, RDD[U]] = {
    val cleanedF = ssc.sparkContext.clean(f)
    process map { _ map cleanedF }
  }

  def transformRDD[U : ClassTag](transformFunc: RDD[T] => RDD[U]): Process[F, RDD[U]] = {
    val cleanedF = ssc.sparkContext.clean(transformFunc)
    process map transformFunc
  }

  /**
   * Return a new RDDProcess in which each RDD has a single element generated by counting each RDD
   * of this DStream.
   */
  def countRDD(): Process[F, RDD[Long]] = {
    process
      .map( _.map(_ => (null, 1L)).union(ssc.sparkContext.makeRDD(Seq((null, 0L)), 1)) )
      .reduceByKey(_ + _)
      .map( _.map(_._2) )
  }

  /**
   * Return a new RDDProcess in which each RDD contains the counts of each distinct value in
   * each RDD of this DStream. Hash partitioning is used to generate
   * the RDDs with `numPartitions` partitions (Spark's default number of partitions if
   * `numPartitions` not specified).
   */
  def countRDDByValue(numPartitions: Int = ssc.sparkContext.defaultParallelism): Process[F, RDD[(T, Long)]] =
    process
      .map{ _.map(x => (x, 1L)) }
      .reduceByKey((x: Long, y: Long) => x + y, numPartitions)

  /**
   * Apply a function to each RDD in this RDDProcess. This is an output operator, so
   * 'this' DStream will be registered as an output stream and therefore materialized.
   */
  def foreachRDD(foreachFunc: RDD[T] => Unit) {
    val cleanedF = ssc.sparkContext.clean(foreachFunc)
    process.map { rdd => foreachFunc(rdd) }
  }

  def reduceRDD(reduceFunc: (T, T) => T): Process[F, RDD[T]] =
    process
      .map(_.map(x => (null, x)))
      .reduceByKey(reduceFunc, 1)
      .map( _.map(_._2) )

}

class ExtendProcessTimeRDD[F[_], T : ClassTag](val process: Process[F, (Long, RDD[T])])(implicit ssc: StreamingContext) {
  import SparkContext._

  import Zpark._
  /**
   * Return a new TimeRDDProcess in which each RDD contains all the elements in seen in a
   * sliding window of time over this DStream.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   */
  def windowRDD(windowDuration: Duration): Process[F, (Long, RDD[T])] = {
    windowRDD(windowDuration, Duration.Zero)
    /*val nanos = windowDuration.toNanos

    def go(acc: Vector[RDD[T]], last: Long): Process1[(Long, RDD[T]),(Long, RDD[T])] =
      Process.await1[(Long, RDD[T])].flatMap { case (current, rdd) =>
        if(current - last > nanos)
          Process.emit((last, new UnionRDD(ssc.sparkContext, acc.toSeq))) ++ go(Vector(rdd), current)
        else go(acc :+ rdd, last)
      } orElse Process.emit((last, new UnionRDD(ssc.sparkContext, acc.toSeq)))

    process |> Process.await1[(Long, RDD[T])].flatMap { case (current, rdd) => go(Vector(rdd), current) }*/
  }

  def windowRDD(windowDuration: Duration, slideDuration: Duration): Process[F, (Long, RDD[T])] = {
    val nanos = windowDuration.toNanos
    val slidingNanos = slideDuration.toNanos

    def sliding(curRDD: RDD[T], curTime: Long, acc: Vector[RDD[T]], last: Long): Process1[(Long, RDD[T]),(Long, RDD[T])] =
      if(curTime - last >= slidingNanos) {
        go(acc :+ curRDD, curTime, last)
      } else {
        Process.await1[(Long, RDD[T])].flatMap { case (current, rdd) => 
          sliding(rdd, current, acc, last) 
        }
      }

    def go(acc: Vector[RDD[T]], origin: Long, last: Long): Process1[(Long, RDD[T]),(Long, RDD[T])] =
      Process.await1[(Long, RDD[T])].flatMap { case (current, rdd) =>
        if(current - origin >= nanos)
          Process.emit((origin, new UnionRDD(ssc.sparkContext, acc.toSeq))) ++ sliding(rdd, current, Vector(), last)
        else go(acc :+ rdd, origin, current)
      } orElse Process.emit((origin, new UnionRDD(ssc.sparkContext, acc.toSeq)))

    process |> Process.await1[(Long, RDD[T])].flatMap { case (current, rdd) => go(Vector(rdd), current, current) }
  }

  def reduceByWindow(
    reduceFunc: (T, T) => T,
    windowDuration: Duration,
    slideDuration: Duration
  ): Process[F, (Long, RDD[T])] = {
    val cleanedReduceFunc = ssc.sparkContext.clean(reduceFunc)
    //process.reduce(reduceFunc).windowRDD(windowDuration, slideDuration).reduce(reduceFunc)

    (new ExtendProcessTimeRDD(process.map { case (time, rdd) =>
      ( time,
        rdd.map(x => (null, x)).combineByKey[T](
          (t: T) => t, cleanedReduceFunc, cleanedReduceFunc, defaultPartitioner
        )
      )
    })).windowRDD(windowDuration, slideDuration).map { case (time, rdd) =>
      ( time,
        rdd.combineByKey[T](
          (t: T) => t, cleanedReduceFunc, cleanedReduceFunc, defaultPartitioner
        ).map{ case (_, x) => x }
      )
    }
  }

}