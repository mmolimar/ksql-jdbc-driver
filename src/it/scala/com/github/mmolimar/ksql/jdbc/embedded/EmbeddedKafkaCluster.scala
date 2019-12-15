package com.github.mmolimar.ksql.jdbc.embedded

import java.io.File
import java.util.Properties

import com.github.mmolimar.ksql.jdbc.utils.TestUtils
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.Logging
import kafka.zk.AdminZkClient

import scala.collection.Seq

class EmbeddedKafkaCluster(zkConnection: String,
                           ports: Seq[Int] = Seq(TestUtils.getAvailablePort),
                           baseProps: Properties = new Properties) extends Logging {

  private val actualPorts: Seq[Int] = ports.map(resolvePort)

  private var brokers: Seq[KafkaServer] = Seq.empty
  private var logDirs: Seq[File] = Seq.empty

  private lazy val zkClient = TestUtils.buildZkClient(zkConnection)
  private lazy val adminZkClient = new AdminZkClient(zkClient)

  def startup(): Unit = {
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

  def shutdown(): Unit = {
    brokers.foreach(broker => TestUtils.swallow(broker.shutdown))
    logDirs.foreach(logDir => TestUtils.swallow(TestUtils.deleteFile(logDir)))
  }

  def getPorts: Seq[Int] = actualPorts

  def getBrokerList: String = actualPorts.map("localhost:" + _).mkString(",")

  def createTopic(topic: String, numPartitions: Int = 1, replicationFactor: Int = 1): Unit = {
    info(s"Creating topic $topic")
    adminZkClient.createTopic(topic, numPartitions, replicationFactor)
  }

  def deleteTopic(topic: String) {
    info(s"Deleting topic $topic")
    adminZkClient.deleteTopic(topic)
  }

  def deleteTopics(topics: Seq[String]): Unit = topics.foreach(deleteTopic)

  def existTopic(topic: String): Boolean = zkClient.topicExists(topic)

  def listTopics: Set[String] = zkClient.getAllTopicsInCluster

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
