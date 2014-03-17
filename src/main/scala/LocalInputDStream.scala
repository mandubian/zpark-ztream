/*
 * Copyright 2014 Pascal Voitot (@mandubian)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** I use spark package to hack all those private Spark classes */
package org.apache.spark.streaming

import java.util.concurrent.ArrayBlockingQueue
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag

import akka.actor.{Props, Actor}
import akka.pattern.ask

import org.apache.spark.streaming.util.{RecurringTimer, SystemClock}
import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.rdd.{RDD, BlockRDD}
import org.apache.spark.storage.{BlockId, StorageLevel, StreamBlockId}

import org.apache.spark.streaming.scheduler.{DeregisterReceiver, AddBlocks, RegisterReceiver}
import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.dstream.{StopReceiver, ReportBlock, ReportError}

import org.apache.spark.{SparkException, Logging, SparkEnv}
import org.apache.spark.SparkContext._

import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.dispatch._
import org.apache.spark.storage.BlockId
import org.apache.spark.util.AkkaUtils


/**
 * ... Just a hack of RemoteInputDstream ...
 * Abstract class for defining any [[org.apache.spark.streaming.dstream.InputDStream]]
 * that has to start a receiver on local node to receive external data.
 * Specific implementations of LocalInputDStream must
 * define the `receiver` function that gets the receiver object of type
 * [[org.apache.spark.streaming.dstream.LocalReceiver]] that will be used
 * to receive data.
 * @param ssc_ Streaming context that will execute this input stream
 * @tparam T Class type of the object of this stream
 */
abstract class LocalInputDStream[T: ClassTag](@transient ssc_ : StreamingContext)
  extends InputDStream[T](ssc_) with Logging {

  // This is an unique identifier that is used to match the Znetwork receiver with the
  // corresponding Znetwork input stream.
  val id = ssc_.getNewNetworkStreamId()

  /**
   * Gets the receiver object that will be sent to the worker nodes
   * to receive data. This method needs to defined by any specific implementation
   * of a LocalInputDStream.
   */
  def receiver: LocalReceiver[T]

  // Nothing to start or stop as both taken care of by the ZNetworkInputTracker.
  override def start() {
    receiver.setStreamId(id)
    receiver.start()
  }

  override def stop() {
    receiver.stop()
  }

  override def compute(validTime: Time): Option[RDD[T]] = {
    // If this is called for any time before the start time of the context,
    // then this returns an empty RDD. This may happen when recovering from a
    // master failure
    if (validTime >= ssc_.graph.startTime) {
      val blockIds = receiver.getBlockIds(validTime)
      logInfo("!!!!!!   GETTING BLOCK"+validTime+ " "+ ssc_.graph.startTime+ " blockIds:"+blockIds.toSeq)
      Some(new BlockRDD[T](ssc_.sc, blockIds))
      //if(!blockIds.isEmpty) Some(new BlockRDD[T](ssc_.sc, blockIds))
      //else None
    } else {
      logInfo("!!!!!!   GETTING BLOCK 2 :"+validTime+ " "+ ssc_.graph.startTime)
      Some(new BlockRDD[T](ssc_.sc, Array[BlockId]()))
    }
  }
}


sealed trait LocalReceiverMessage
case class   ReportBlock(blockId: BlockId, metadata: Any) extends LocalReceiverMessage

// case class GetBlocks(time: Time) extends LocalReceiverMessage

/**
 * Abstract class of a receiver that can be run on worker nodes to receive external data. See
 * [[org.apache.spark.streaming.dstream.LocalInputDStream]] for an explanation.
 */
abstract class LocalReceiver[T: ClassTag]() extends Logging {

  protected lazy val env = SparkEnv.get

  protected lazy val actor = env.actorSystem.actorOf(
    Props(new LocalReceiverActor()), "LocalReceiver-" + streamId)

  protected var streamId: Int = -1

  protected implicit val timeout = new akka.util.Timeout(5 seconds)

  protected val receivedBlockIds = Queue[BlockId]()

  /**
   * This method will be called to start receiving data. All your receiver
   * starting code should be implemented by defining this function.
   */
  protected def onStart()

  /** This method will be called to stop receiving data. */
  protected def onStop()


  /**
   * Starts the receiver. First is accesses all the lazy members to
   * materialize them. Then it calls the user-defined onStart() method to start
   * other threads, etc required to receiver the data.
   */
  def start() {
    try {
      // Access the lazy vals to materialize them
      logInfo("Starting local receiver")
      env
      logInfo("Started actor")

      // Call user-defined onStart()
      onStart()
    } catch {
      //case ie: InterruptedException =>
      //  logInfo("Receiving thread interrupted")
      case e: Exception =>
        stopOnError(e)
    }
  }

  /**
   * Stops the receiver. First it interrupts the main receiving thread,
   * that is, the thread that called receiver.start(). Then it calls the user-defined
   * onStop() method to stop other threads and/or do cleanup.
   */
  def stop() {
    // DO NOT KILL THREAD/ACTOR, WE ARE LOCAL
    logInfo("Stopping local receiver")
    onStop()

  }

  /**
   * Stops the receiver and reports exception to the tracker.
   * This should be called whenever an exception is to be handled on any thread
   * of the receiver.
   */
  def stopOnError(e: Exception) {
    logError("Error receiving data", e)
    stop()
  }

  /** Return all the blocks received from a receiver. */
  def getBlockIds(time: Time): Array[BlockId] = {
    logInfo("Getting blocks")

    val result = receivedBlockIds.synchronized{
      receivedBlockIds.dequeueAll(x => true)
    }
    logInfo("Stream " + LocalReceiver.this.streamId + " received " + result.size + " blocks")
    result.toArray
  }

  /**
   * Pushes a block (as an ArrayBuffer filled with data) into the block manager.
   */
  def pushBlock(blockId: BlockId, arrayBuffer: ArrayBuffer[T], metadata: Any, level: StorageLevel) {
    //logInfo("!!!!! pushing "+blockId)
    env.blockManager.put(blockId, arrayBuffer.asInstanceOf[ArrayBuffer[Any]], level)
    actor ! ReportBlock(blockId, metadata)
  }

  /**
   * Pushes a block (as bytes) into the block manager.
   */
  def pushBlock(blockId: BlockId, bytes: ByteBuffer, metadata: Any, level: StorageLevel) {
    //logInfo("!!!!! pushing2 "+blockId)
    env.blockManager.putBytes(blockId, bytes, level)
    actor ! ReportBlock(blockId, metadata)
  }

  /** A helper actor that communicates with the NetworkInputTracker */
  private class LocalReceiverActor extends Actor {
    logInfo("Attempting to register with tracker")

    override def receive() = {
      case ReportBlock(blockId, metadata) =>
        receivedBlockIds.synchronized {
          receivedBlockIds += blockId
        }
    }
  }

  protected[streaming] def setStreamId(id: Int) {
    streamId = id
  }

  /**
   * Batches objects created by a [[org.apache.spark.streaming.dstream.LocalReceiver]] and puts them into
   * appropriately named blocks at regular intervals. This class starts two threads,
   * one to periodically start a new batch and prepare the previous batch of as a block,
   * the other to push the blocks into the block manager.
   */
  class BlockGenerator(storageLevel: StorageLevel)
    extends Serializable with Logging {

    case class Block(id: BlockId, buffer: ArrayBuffer[T], metadata: Any = null)

    val clock = new SystemClock()
    val blockInterval = env.conf.getLong("spark.streaming.blockInterval", 200)
    val blockIntervalTimer = new RecurringTimer(clock, blockInterval, updateCurrentBuffer)
    val blockStorageLevel = storageLevel
    val blocksForPushing = new ArrayBlockingQueue[Block](1000)
    val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }

    var currentBuffer = new ArrayBuffer[T]

    var goon = true

    def start() {
      blockIntervalTimer.start()
      blockPushingThread.start()
      logInfo("Data handler started")
    }

    def stop() {
      blockIntervalTimer.stop()
      updateCurrentBuffer(System.currentTimeMillis())

      goon = false
      //blockPushingThread.interrupt()
      logInfo("Data handler stopped")
    }

    def += (obj: T): Unit = synchronized {
      currentBuffer += obj
      //logInfo("***** ----> currentBuffer:"+currentBuffer)
    }

    private def updateCurrentBuffer(time: Long): Unit = synchronized {
      try {
        //logInfo("----> currentBuffer:"+currentBuffer)
        val newBlockBuffer = currentBuffer
        currentBuffer = new ArrayBuffer[T]
        //logInfo("----> newBlockBuffer:"+newBlockBuffer)
        if (newBlockBuffer.size > 0) {
          val blockId = StreamBlockId(LocalReceiver.this.streamId, time - blockInterval)
          val newBlock = new Block(blockId, newBlockBuffer)
          blocksForPushing.add(newBlock)
        }
      } catch {
        case ie: InterruptedException =>
          logInfo("Block interval timer thread interrupted")
        case e: Exception =>
          LocalReceiver.this.stop()
      }
    }

    private def keepPushingBlocks() {
      logInfo("Block pushing thread started")
      try {
        while(goon) {
          val block = blocksForPushing.take()
          logInfo("Block pushing :"+block)
          LocalReceiver.this.pushBlock(block.id, block.buffer, block.metadata, storageLevel)
        }
      } catch {
        case ie: InterruptedException =>
          logInfo("Block pushing thread interrupted")
          // logInfo("Last Blocks:"+blocksForPushing)
          // if(blocksForPushing.size() > 0) {
          //   val block = blocksForPushing.take()
          //   LocalReceiver.this.pushBlock(block.id, block.buffer, block.metadata, storageLevel)
          // }
        case e: Exception =>
          LocalReceiver.this.stop()
      }
    }
  }
}
