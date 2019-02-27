package com.github.mmolimar.ksql.jdbc.embedded

import java.io.IOException

import com.github.mmolimar.ksql.jdbc.utils.TestUtils
import io.confluent.ksql.rest.server.{KsqlRestApplication, KsqlRestConfig}
import io.confluent.ksql.version.metrics.VersionCheckerAgent
import kafka.utils.Logging
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalamock.scalatest.MockFactory

import scala.collection.JavaConversions._

class EmbeddedKsqlEngine(brokerList: String, port: Int = TestUtils.getAvailablePort) extends Logging with MockFactory {

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

  import java.util.function.{Function => JFunction, Supplier => JSupplier}

  implicit def toJavaSupplier[A](f: Function0[A]) = new JSupplier[A] {
    override def get: A = f()
  }

  implicit def toJavaFunction[A, B](f: Function1[A, B]) = new JFunction[A, B] {
    override def apply(a: A): B = f(a)
  }

  lazy val ksqlEngine = {
    val versionCheckerAgent = mock[VersionCheckerAgent]
    (versionCheckerAgent.start _).expects(*, *).returns().anyNumberOfTimes
    (versionCheckerAgent.updateLastRequestTime _).expects().returns().anyNumberOfTimes
    KsqlRestApplication.buildApplication(config, (_: JSupplier[java.lang.Boolean]) => versionCheckerAgent)
  }

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
