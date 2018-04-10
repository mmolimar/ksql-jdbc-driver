package com.github.mmolimar.ksql.jdbc.embedded

import java.io.IOException

import com.github.mmolimar.ksql.jdbc.utils.TestUtils
import io.confluent.ksql.rest.server.{KsqlRestApplication, KsqlRestConfig}
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent
import kafka.utils.Logging
import org.apache.kafka.clients.producer.ProducerConfig

import scala.collection.JavaConversions._

class EmbeddedKsqlEngine(brokerList: String, port: Int = TestUtils.getAvailablePort) extends Logging {

  val config = new KsqlRestConfig(Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
    "listeners" -> s"http://localhost:$port",
    "ksql.cluster.id" -> "ksql-jdbc",
    "application.id" -> "test",
    "ksql.command.consumer.client.id" -> "ksql-jdbc-driver",
    "ksql.command.consumer.auto.offset.reset" -> "earliest",
    "ksql.command.consumer.session.timeout.ms" -> "10000",
    "ksql.command.topic.suffix" -> "commands"
  ))

  lazy val ksqlEngine = KsqlRestApplication.buildApplication(config, true, new KsqlVersionCheckerAgent)

  @throws[IOException]
  def startup = {
    info("Starting up embedded KSQL engine")

    ksqlEngine.start

    info("Started embedded Zookeeper: " + getConnection)
  }

  def shutdown = {
    info("Shutting down embedded KSQL engine")

    TestUtils.swallow(ksqlEngine.stop)

    info("Shutted down embedded KSQL engine")
  }

  def getPort: Int = port

  def getConnection: String = "localhost:" + getPort

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder("KSQL{")
    sb.append("connection=").append(getConnection)
    sb.append('}')

    sb.toString
  }

}
