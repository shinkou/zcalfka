// vim: set sw=2 ts=2 tw=0 et:
package com.github.shinkou.zcalfka.consumer

import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._

import kafka.api.{FetchRequestBuilder, PartitionOffsetRequestInfo, OffsetRequest, OffsetResponse, Request}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.utils.{Json, ZKStringSerializer}

import org.I0Itec.zkclient.ZkClient;

import org.apache.log4j.{Logger, LogManager}

class BaseConsumer(val gid: String, val t: String, val ep: String) {
  protected var logger = LogManager.getLogger(getClass)
  protected var groupId = gid
  protected var topic = t
  protected var fetchSize = BaseConsumer.FETCH_SIZE
  protected var soTimeout = BaseConsumer.SO_TIMEOUT
  protected var endpoInt = ep
  protected var sessionTimeout = BaseConsumer.ZK_TIMEOUT
  protected var connectionTimeout = BaseConsumer.CNX_TIMEOUT
  protected var offsetMark = BaseConsumer.LATEST_OFFSET
  protected var saveInterval = BaseConsumer.SAVE_INTERVAL
  protected var kafkaReconnectWait = BaseConsumer.KAFKA_RECONNECT_WAIT
  protected var zkClient = new ZkClient(endpoInt, sessionTimeout, connectionTimeout, ZKStringSerializer)
  protected var consumers = scala.collection.mutable.Map[Int, SimpleConsumer]()
  protected var clientnames = scala.collection.mutable.Map[Int, String]()
  @volatile protected var running = true

  protected def getClientname(pid: Int, regen: Boolean): String = {
    if (regen) {
      var s = "Client_" + topic + "_" + pid.toString + "_" + java.lang.System.currentTimeMillis.toString
      clientnames.put(pid, s)
      return s
    }
    var clientname: String = clientnames.get(pid) match {
      case Some(name) => name
      case None => {
        var s = "Client_" + topic + "_" + pid.toString + "_" + java.lang.System.currentTimeMillis.toString
        clientnames.put(pid, s)
        s
      }
    }
    clientname
  }

  protected def getClientname(pid: Int): String = {
    getClientname(pid, false)
  }

  protected def connectKafka(h: String, p: Int, pid: Int) = new SimpleConsumer(h, p, soTimeout, fetchSize, getClientname(pid))

  protected def getOffset(pid: Int) = {
    var consumer = consumers.get(pid)
    var offset = 0L
    consumer match {
      case Some(c) => {
        var clientname = getClientname(pid)
        var tap = new TopicAndPartition(topic, pid)
        var reqInfo = scala.collection.mutable.Map[TopicAndPartition, PartitionOffsetRequestInfo]()
        reqInfo.put(tap, PartitionOffsetRequestInfo(offsetMark, 1))
        var req = new OffsetRequest(reqInfo.toMap, OffsetRequest.CurrentVersion, Request.OrdinaryConsumerId, clientname)
        var res = c.getOffsetsBefore(req)
        if (! res.hasError) {
          var offsets = res.partitionErrorAndOffsets(TopicAndPartition(topic, pid)).offsets.toArray
          offset = offsets(0)
        }
      }
      case None =>
    }
    offset
  }

  protected def getZkBrokerPath() = "/brokers/topics/" + topic

  protected def getZkBrokerPath(bid: Int) = "/brokers/ids/" + bid.toString

  protected def getZkBrokerPartitionPath(pid: Int) = getZkBrokerPath + "/partitions/" + pid.toString

  protected def getZkConsumerPath() = "/consumers/" + groupId

  protected def getZkConsumerOwnerPath() = getZkConsumerPath + "/owners/" + topic

  protected def getZkConsumerOffsetPath(pid: Int) = getZkConsumerPath + "/offsets/" + topic + "/" + pid.toString

  protected def connectZk() = {
    zkClient = new ZkClient(endpoInt, sessionTimeout, connectionTimeout, ZKStringSerializer)
  }

  protected def loadPartitions(): List[Int] = {
    var path = getZkBrokerPath + "/partitions"
    var partitions = List[Int]()
    if (zkClient.exists(path)) partitions = for(s <- zkClient.getChildren(path).toList) yield s.toInt
    partitions
  }

  protected def loadBrokerAddress(bid: Int): Map[String, Any] = {
    var path = getZkBrokerPath(bid)
    var brokerinfo = Map[String, Any]()
    if (zkClient.exists(path)) {
      var s: String = zkClient.readData(path)
      brokerinfo = Json.parseFull(s).get.asInstanceOf[HashMap[String, Any]].toMap
    }
    brokerinfo
  }

  // protected def saveOffset(pid: Int, offset: long)

  protected def loadLeaderAddresses(pid: Int) = {
    var path = getZkBrokerPartitionPath(pid) + "/state"
    var brokerinfo = Map[String, Any]()
    if (zkClient.exists(path)) {
      var s: String = zkClient.readData(path)
      var leaderinfo = Json.parseFull(s).get.asInstanceOf[HashMap[String, Any]]
      brokerinfo = leaderinfo.get("leader") match {
        case Some(a) => loadBrokerAddress(a.asInstanceOf[Int])
        case None => Map[String, Any]()
      }
    }
    brokerinfo
  }

  protected def connectKafka(pid: Int): SimpleConsumer = {
    var brokerinfo = loadLeaderAddresses(pid)
    connectKafka(brokerinfo.get("host").orNull.asInstanceOf[String], brokerinfo.get("port").orNull.asInstanceOf[Int], pid)
  }

  def start() = {
    var partitions = loadPartitions()
    partitions.par.foreach((partition) => {
      var consumer = connectKafka(partition)
      consumers.put(partition, consumer)
      var curOffset = getOffset(partition)
      var savemark = java.lang.System.currentTimeMillis
      while(running) {
        try {
          var req = new FetchRequestBuilder().clientId(getClientname(partition)).addFetch(topic, partition, curOffset, fetchSize).build
          var res = consumer.fetch(req)
          if (res.hasError) {
            if (res.errorCode(topic, partition) == ErrorMapping.OffsetOutOfRangeCode) {
              logger.warn("Invalid offset: " + curOffset) // to be removed
              curOffset = getOffset(partition)
              logger.warn("Valid offset obtained: " + curOffset) // to be removed
            }
            else {
              logger.warn("Errors detected in fetch response. Code: " + res.errorCode(topic, partition))
              consumer.close
              consumer = connectKafka(partition)
              consumers.put(partition, consumer)
              getClientname(partition, true)
            }
          }
          else {
            for(mao <- res.messageSet(topic, partition)) {
              if (mao.offset < curOffset) {
                logger.warn("Found an old offset: " + mao.offset + " Expecting: " + curOffset)
              }
              else {
                curOffset = mao.nextOffset
                process(mao.message.payload)
              }
            }
            if (java.lang.System.currentTimeMillis - savemark > saveInterval) {
              // we are supposed to save the current offset here
              logger.info("Partition: " + partition + "  Offset: " + curOffset)
              savemark = java.lang.System.currentTimeMillis
            }
          }
        }
        catch {
          case e: java.nio.channels.ClosedChannelException => {
            logger.error("Underlying socket closed.")
          }
          case e: java.nio.channels.ClosedByInterruptException => {
            logger.error("Socket closed by interrupt.")
          }
          case e: Exception => {
            logger.error("Unknown exception.")
          }
          logger.error(e)
          consumer.close
          Thread.sleep(kafkaReconnectWait)
          consumer = connectKafka(partition)
          consumers.put(partition, consumer)
        }
      }
    })
  }

  def stop() {
    running = false
    consumers.foreach((e: (Int, SimpleConsumer)) => {
      e._2.close
      logger.info("Partition " + e._1 + " consumer closed.")
    })
  }

  def process(buf: java.nio.ByteBuffer) {
    var arr = new Array[Byte](buf.limit)
    buf.get(arr)
    var s = new String(arr, "UTF-8")
    logger.info(s)
  }
}

object BaseConsumer {
  val FETCH_SIZE = 1024
  val SO_TIMEOUT = 30000
  val ZK_TIMEOUT = 10000
  val CNX_TIMEOUT = 10000
  val LATEST_OFFSET = -1L
  val EARLIEST_OFFSET = -2L
  val SAVE_INTERVAL = 300000
  val KAFKA_RECONNECT_WAIT = 1000
}
