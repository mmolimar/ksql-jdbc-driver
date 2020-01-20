package com.github.mmolimar.ksql.jdbc.embedded

import java.util

import com.github.mmolimar.ksql.jdbc.utils.TestUtils
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy
import org.apache.kafka.connect.runtime.isolation.Plugins
import org.apache.kafka.connect.runtime.rest.RestServer
import org.apache.kafka.connect.runtime.standalone.{StandaloneConfig, StandaloneHerder}
import org.apache.kafka.connect.runtime.{Connect, Worker, WorkerConfig}
import org.apache.kafka.connect.storage.FileOffsetBackingStore
import org.apache.kafka.connect.util.ConnectUtils

import scala.collection.JavaConverters._
import scala.reflect.io.File

class EmbeddedKafkaConnect(brokerList: String, port: Int = TestUtils.getAvailablePort) extends Logging {

  private val workerProps: util.Map[String, String] = Map[String, String](
    WorkerConfig.LISTENERS_CONFIG -> s"http://localhost:$port",
    WorkerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
    WorkerConfig.KEY_CONVERTER_CLASS_CONFIG -> "org.apache.kafka.connect.converters.ByteArrayConverter",
    WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG -> "org.apache.kafka.connect.converters.ByteArrayConverter",
    StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG -> File.makeTemp(prefix = "connect.offsets").jfile.getAbsolutePath
  ).asJava

  private lazy val kafkaConnect: Connect = buildConnect

  def startup(): Unit = {
    info("Starting up embedded Kafka connect")

    kafkaConnect.start()

    info(s"Started embedded Kafka connect on port: $port")
  }

  def shutdown(): Unit = {
    info("Shutting down embedded Kafka Connect")

    TestUtils.swallow(kafkaConnect.stop())

    info("Stopped embedded Kafka Connect")
  }

  private def buildConnect: Connect = {
    val config = new StandaloneConfig(workerProps)
    val kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config)

    val rest = new RestServer(config)
    rest.initializeServer()

    val advertisedUrl = rest.advertisedUrl
    val workerId = advertisedUrl.getHost + ":" + advertisedUrl.getPort
    val plugins = new Plugins(workerProps)
    val connectorClientConfigOverridePolicy = plugins.newPlugin(
      config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG), config, classOf[ConnectorClientConfigOverridePolicy])
    val worker = new Worker(workerId, Time.SYSTEM, plugins, config, new FileOffsetBackingStore, connectorClientConfigOverridePolicy)
    val herder = new StandaloneHerder(worker, kafkaClusterId, connectorClientConfigOverridePolicy)

    new Connect(herder, rest)
  }

  def getPort: Int = port

  def getWorker: String = s"localhost:$port"

  def getUrl: String = s"http://localhost:$port"

  override def toString: String = {
    val sb: StringBuilder = StringBuilder.newBuilder
    sb.append("KafkaConnect{")
    sb.append("port=").append(port)
    sb.append('}')

    sb.toString
  }

}
