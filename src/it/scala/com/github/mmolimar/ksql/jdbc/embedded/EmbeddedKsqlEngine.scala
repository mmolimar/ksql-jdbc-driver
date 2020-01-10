package com.github.mmolimar.ksql.jdbc.embedded

import java.io.IOException

import com.github.mmolimar.ksql.jdbc.utils.TestUtils
import io.confluent.ksql.rest.server.{KsqlRestApplication, KsqlRestConfig}
import io.confluent.ksql.version.metrics.VersionCheckerAgent
import io.confluent.rest.RestConfig
import kafka.utils.Logging
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalamock.scalatest.MockFactory
import io.confluent.ksql.util.KsqlConfig

import scala.collection.JavaConverters._

class EmbeddedKsqlEngine(port: Int = TestUtils.getAvailablePort, brokerList: String, connectUrl: String) extends Logging with MockFactory {

  private val config = new KsqlRestConfig(Map(
    RestConfig.LISTENERS_CONFIG -> s"http://localhost:$port",
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
    KsqlConfig.CONNECT_URL_PROPERTY -> connectUrl,
    "ksql.service.id" -> "ksql-jdbc",
    "ksql.streams.auto.offset.reset" -> "latest",
    "ksql.command.topic.suffix" -> "commands"
  ).asJava)

  lazy val ksqlEngine: KsqlRestApplication = {
    import io.confluent.ksql.rest.server.mock.ksqlRestApplication

    val versionCheckerAgent = mock[VersionCheckerAgent]
    (versionCheckerAgent.start _).expects(*, *).returns((): Unit).anyNumberOfTimes
    (versionCheckerAgent.updateLastRequestTime _).expects().returns((): Unit).anyNumberOfTimes
    ksqlRestApplication(config, versionCheckerAgent)
  }

  @throws[IOException]
  def startup(): Unit = {
    info("Starting up embedded KSQL engine")

    ksqlEngine.start()

    info("Started embedded Zookeeper: " + getConnection)
  }

  def shutdown(): Unit = {
    info("Shutting down embedded KSQL engine")

    TestUtils.swallow(ksqlEngine.stop())

    info("Stopped embedded KSQL engine")
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
