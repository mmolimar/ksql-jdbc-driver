package com.github.mmolimar.ksql.jdbc

import java.sql.{SQLException, SQLFeatureNotSupportedException}
import java.util.Properties

import com.github.mmolimar.ksql.jdbc.utils.TestUtils.MockableKsqlRestClient
import io.confluent.ksql.rest.client.{KsqlRestClient, RestResponse}
import io.confluent.ksql.rest.entity.{KsqlErrorMessage, ServerInfo}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class KsqlDriverSpec extends WordSpec with Matchers with MockFactory {

  "A KsqlDriver" when {
    val driver = new KsqlDriver

    "validating specs" should {
      "not be JDBC compliant" in {
        driver.jdbcCompliant should be(false)
      }
      "have a major and minor version" in {
        driver.getMinorVersion should be(3)
        driver.getMajorVersion should be(0)
      }
      "have no properties" in {
        driver.getPropertyInfo("", new Properties).length should be(0)
      }
      "throw an exception when getting parent logger" in {
        assertThrows[SQLFeatureNotSupportedException] {
          driver.getParentLogger
        }
      }
      "throw an exception when connecting to an invalid URL" in {
        assertThrows[SQLException] {
          driver.connect("invalid", new Properties)
        }
        assertThrows[SQLException] {
          driver.connect("jdbc:ksql://localhost:9999999", new Properties)
        }
      }
    }

    "connecting to an URL" should {
      val mockKsqlRestClient = mock[MockableKsqlRestClient]
      val driver = new KsqlDriver {
        override private[jdbc] def buildConnection(values: KsqlConnectionValues, properties: Properties) = {
          new KsqlConnection(values, new Properties) {
            override def init: KsqlRestClient = mockKsqlRestClient
          }
        }
      }
      "throw an exception if cannot connect to the URL" in {
        assertThrows[SQLException] {
          (mockKsqlRestClient.makeRootRequest _).expects()
            .throws(new Exception("error"))
            .once
          driver.connect("jdbc:ksql://localhost:9999", new Properties)
        }
      }
      "throw an exception if there is an error in the response" in {
        assertThrows[SQLException] {
          (mockKsqlRestClient.makeRootRequest _).expects()
            .returns(RestResponse.erroneous(new KsqlErrorMessage(-1, "error message", List.empty.asJava)))
            .once
          driver.connect("jdbc:ksql://localhost:9999", new Properties)
        }
      }
      "connect properly if the response is successful" in {
        (mockKsqlRestClient.makeRootRequest _).expects()
          .returns(RestResponse.successful[ServerInfo](new ServerInfo("v1", "id1", "svc1")))
          .once
        val connection = driver.connect("jdbc:ksql://localhost:9999", new Properties)
        connection.isClosed should be(false)
      }
    }

    "accepting an URL" should {
      "return false if invalid" in {
        driver.acceptsURL(null) should be(false)
        driver.acceptsURL("") should be(false)
        driver.acceptsURL("jdbc:invalid://ksql-server:8080") should be(false)
      }
      "return true if valid" in {
        driver.acceptsURL("jdbc:ksql://ksql-server:8080") should be(true)
        driver.acceptsURL("jdbc:ksql://") should be(true)
      }
    }

    "parsing an URL" should {
      "throw an SQLException if invalid" in {
        assertThrows[SQLException] {
          KsqlDriver.parseUrl(null)
        }
        assertThrows[SQLException] {
          KsqlDriver.parseUrl("")
        }
        assertThrows[SQLException] {
          KsqlDriver.parseUrl("jdbc:invalid://ksql-server:8080")
        }
      }
      "return the URL parsed properly" in {
        val ksqlServer = "ksql-server"
        val ksqlPort = 8080
        val ksqlUrl = s"http://${ksqlServer}:${ksqlPort}"
        val ksqlUrlSecured = s"https://${ksqlServer}:${ksqlPort}"

        var url = s"jdbc:ksql://${ksqlServer}:${ksqlPort}"
        var connectionValues = KsqlDriver.parseUrl(url)
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.isEmpty should be(true)
        connectionValues.ksqlUrl should be(ksqlUrl)
        connectionValues.jdbcUrl should be(url)
        connectionValues.isSecured should be(false)
        connectionValues.properties should be(false)
        connectionValues.timeout should be(0)

        url = s"jdbc:ksql://${ksqlServer}:${ksqlPort}?prop1=value1"
        connectionValues = KsqlDriver.parseUrl(url)
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.size should be(1)
        connectionValues.config.get("prop1").get should be("value1")
        connectionValues.ksqlUrl should be(ksqlUrl)
        connectionValues.jdbcUrl should be(url)
        connectionValues.isSecured should be(false)
        connectionValues.properties should be(false)
        connectionValues.timeout should be(0)

        url = s"jdbc:ksql://${ksqlServer}:${ksqlPort}?prop1=value1&secured=true&prop2=value2"
        connectionValues = KsqlDriver.parseUrl(url)
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.size should be(3)
        connectionValues.config.get("prop1").get should be("value1")
        connectionValues.config.get("prop2").get should be("value2")
        connectionValues.config.get("secured").get should be("true")
        connectionValues.ksqlUrl should be(ksqlUrlSecured)
        connectionValues.jdbcUrl should be(url)
        connectionValues.isSecured should be(true)
        connectionValues.properties should be(false)
        connectionValues.timeout should be(0)

        url = s"jdbc:ksql://${ksqlServer}:${ksqlPort}?prop1=value1&timeout=100&prop2=value2"
        connectionValues = KsqlDriver.parseUrl(url)
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.size should be(3)
        connectionValues.config.get("prop1").get should be("value1")
        connectionValues.config.get("prop2").get should be("value2")
        connectionValues.config.get("timeout").get should be("100")
        connectionValues.ksqlUrl should be(ksqlUrl)
        connectionValues.jdbcUrl should be(url)
        connectionValues.isSecured should be(false)
        connectionValues.properties should be(false)
        connectionValues.timeout should be(100)

        url = s"jdbc:ksql://${ksqlServer}:${ksqlPort}?prop1=value1&properties=true&prop2=value2"
        connectionValues = KsqlDriver.parseUrl(url)
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.size should be(3)
        connectionValues.config.get("prop1").get should be("value1")
        connectionValues.config.get("prop2").get should be("value2")
        connectionValues.config.get("properties").get should be("true")
        connectionValues.ksqlUrl should be(ksqlUrl)
        connectionValues.jdbcUrl should be(url)
        connectionValues.isSecured should be(false)
        connectionValues.properties should be(true)
        connectionValues.timeout should be(0)

        url = s"jdbc:ksql://${ksqlServer}:${ksqlPort}?timeout=100&secured=true&properties=true&prop1=value1"
        connectionValues = KsqlDriver.parseUrl(url)
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.size should be(4)
        connectionValues.config.get("prop1").get should be("value1")
        connectionValues.config.get("timeout").get should be("100")
        connectionValues.config.get("secured").get should be("true")
        connectionValues.config.get("properties").get should be("true")
        connectionValues.ksqlUrl should be(ksqlUrlSecured)
        connectionValues.jdbcUrl should be(url)
        connectionValues.isSecured should be(true)
        connectionValues.properties should be(true)
        connectionValues.timeout should be(100)
      }
    }
  }
}
