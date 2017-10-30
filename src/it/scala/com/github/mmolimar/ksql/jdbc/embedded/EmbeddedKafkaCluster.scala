package com.github.mmolimar.ksql.jdbc.embedded

import java.io.File
import java.util.Properties

import com.github.mmolimar.ksql.jdbc.utils.TestUtils
import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{Logging, ZkUtils}

class EmbeddedKafkaCluster(zkConnection: String,
                           ports: Seq[Int] = Seq(TestUtils.getAvailablePort),
                           baseProps: Properties = new Properties) extends Logging {

  private val actualPorts: Seq[Int] = ports.map(resolvePort(_))

  private var brokers: Seq[KafkaServer] = Seq.empty
  private var logDirs: Seq[File] = Seq.empty

  def startup = {
    info("Starting up embedded Kafka brokers")

    for ((port, i) <- actualPorts.zipWithIndex) {
      val logDir: File = TestUtils.constructTempDir("kafka-local")

      val properties: Properties = new Properties(baseProps)
      properties.setProperty(KafkaConfig.ZkConnectProp, zkConnection)
      properties.setProperty(KafkaConfig.ZkSyncTimeMsProp, i.toString)
      properties.setProperty(KafkaConfig.BrokerIdProp, (i + 1).toString)
      properties.setProperty(KafkaConfig.HostNameProp, "localhost")
      properties.setProperty(KafkaConfig.AdvertisedHostNameProp, "localhost")
      properties.setProperty(KafkaConfig.PortProp, port.toString)
      properties.setProperty(KafkaConfig.AdvertisedPortProp, port.toString)
      properties.setProperty(KafkaConfig.LogDirProp, logDir.getAbsolutePath)
      properties.setProperty(KafkaConfig.NumPartitionsProp, 1.toString)
      properties.setProperty(KafkaConfig.AutoCreateTopicsEnableProp, true.toString)
      properties.setProperty(KafkaConfig.DeleteTopicEnableProp, true.toString)
      properties.setProperty(KafkaConfig.LogFlushIntervalMessagesProp, 1.toString)
      properties.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, 1.toString)

      info(s"Local directory for broker ID ${i + 1} is ${logDir.getAbsolutePath}")

      brokers :+= startBroker(properties)
      logDirs :+= logDir
    }

    info(s"Started embedded Kafka brokers: $getBrokerList")
  }

  def shutdown = {
    brokers.foreach(broker => TestUtils.swallow(broker.shutdown))
    logDirs.foreach(logDir => TestUtils.swallow(TestUtils.deleteFile(logDir)))
  }

  def getPorts: Seq[Int] = actualPorts

  def getBrokerList: String = actualPorts.map("localhost:" + _).mkString(",")

  def createTopic(topic: String, numPartitions: Int = 1, replicationFactor: Int = 1) = {
    info(s"Creating topic $topic")
    AdminUtils.createTopic(getZkUtils, topic, numPartitions, replicationFactor)
  }

  def deleteTopic(topic: String) {
    info(s"Deleting topic $topic")
    AdminUtils.deleteTopic(getZkUtils, topic)
  }

  def deleteTopics(topics: Seq[String]) = topics.foreach(deleteTopic(_))

  def existTopic(topic: String): Boolean = AdminUtils.topicExists(getZkUtils, topic)

  def listTopics = getZkUtils.getAllTopics

  private def getZkUtils: ZkUtils = if (brokers.isEmpty) null else brokers.head.zkUtils

  private def resolvePort(port: Int) = if (port <= 0) TestUtils.getAvailablePort else port

  private def startBroker(props: Properties): KafkaServer = {
    val server = new KafkaServer(new KafkaConfig(props))
    server.startup
    server
  }

  override def toString: String = {
    val sb: StringBuilder = StringBuilder.newBuilder
    sb.append("Kafka{")
    sb.append("brokerList='").append(getBrokerList).append('\'')
    sb.append('}')

    sb.toString
  }

}
