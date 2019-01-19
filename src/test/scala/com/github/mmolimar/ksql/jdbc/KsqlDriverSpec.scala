package com.github.mmolimar.ksql.jdbc

import java.sql.{SQLException, SQLFeatureNotSupportedException}
import java.util.Properties

import org.scalatest.{Matchers, WordSpec}

class KsqlDriverSpec extends WordSpec with Matchers {

  "A KsqlDriver" when {
    val driver = new KsqlDriver
    "validating specs" should {
      "not be JDBC compliant" in {
        driver.jdbcCompliant shouldBe (false)
      }
      "have a major and minor version" in {
        driver.getMinorVersion shouldBe (0)
        driver.getMajorVersion shouldBe (1)
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
    "accepting an URL" should {
      val driver = new KsqlDriver
      "return false if invalid" in {
        driver.acceptsURL(null) shouldBe (false)
        driver.acceptsURL("") shouldBe (false)
        driver.acceptsURL("jdbc:invalid://ksql-server:8080") shouldBe (false)
      }
      "return true if valid" in {
        driver.acceptsURL("jdbc:ksql://ksql-server:8080") shouldBe (true)
        driver.acceptsURL("jdbc:ksql://") shouldBe (true)
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

        var connectionValues = KsqlDriver.parseUrl(s"jdbc:ksql://${ksqlServer}:${ksqlPort}")
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.isEmpty should be(true)
        connectionValues.getKsqlUrl should be(s"http://${ksqlServer}:${ksqlPort}")
        connectionValues.isSecured should be(false)
        connectionValues.timeout should be(0)

        connectionValues = KsqlDriver.parseUrl(s"jdbc:ksql://${ksqlServer}:${ksqlPort}?prop1=value1")
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.size should be(1)
        connectionValues.config.get("prop1").get should be("value1")
        connectionValues.getKsqlUrl should be(s"http://${ksqlServer}:${ksqlPort}")
        connectionValues.isSecured should be(false)
        connectionValues.timeout should be(0)

        connectionValues = KsqlDriver.parseUrl(s"jdbc:ksql://${ksqlServer}:${ksqlPort}?prop1=value1&secured=true&prop2=value2")
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.size should be(3)
        connectionValues.config.get("prop1").get should be("value1")
        connectionValues.config.get("prop2").get should be("value2")
        connectionValues.config.get("secured").get should be("true")
        connectionValues.getKsqlUrl should be(s"https://${ksqlServer}:${ksqlPort}")
        connectionValues.isSecured should be(true)
        connectionValues.timeout should be(0)

        connectionValues = KsqlDriver.parseUrl(s"jdbc:ksql://${ksqlServer}:${ksqlPort}?prop1=value1&timeout=100&prop2=value2")
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.size should be(3)
        connectionValues.config.get("prop1").get should be("value1")
        connectionValues.config.get("prop2").get should be("value2")
        connectionValues.config.get("timeout").get should be("100")
        connectionValues.getKsqlUrl should be(s"http://${ksqlServer}:${ksqlPort}")
        connectionValues.isSecured should be(false)
        connectionValues.timeout should be(100)

        connectionValues = KsqlDriver.parseUrl(s"jdbc:ksql://${ksqlServer}:${ksqlPort}?timeout=100&secured=true&prop1=value1")
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.size should be(3)
        connectionValues.config.get("prop1").get should be("value1")
        connectionValues.config.get("timeout").get should be("100")
        connectionValues.config.get("secured").get should be("true")
        connectionValues.getKsqlUrl should be(s"https://${ksqlServer}:${ksqlPort}")
        connectionValues.isSecured should be(true)
        connectionValues.timeout should be(100)
      }
    }
  }

}
