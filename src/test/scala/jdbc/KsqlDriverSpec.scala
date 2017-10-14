package jdbc

import java.sql.{SQLException, SQLFeatureNotSupportedException}

import com.github.mmolimar.ksql.jdbc.KsqlDriver
import org.scalatest.{Matchers, WordSpec}

class KsqlDriverSpec extends WordSpec with Matchers {

  "A KsqlDriver" when {
    val driver = new KsqlDriver()
    "validating specs" should {
      "not be JDBC compliant" in {
        driver.jdbcCompliant shouldBe (false)
      }
      "have a major and minor version" in {
        driver.getMinorVersion shouldBe (0)
        driver.getMajorVersion shouldBe (1)
      }
      "throw an exception when getting parent logger" in {
        assertThrows[SQLFeatureNotSupportedException] {
          driver.getParentLogger
        }
      }
    }
    "accepting an URL" should {
      val driver = new KsqlDriver()
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

        connectionValues = KsqlDriver.parseUrl(s"jdbc:ksql://${ksqlServer}:${ksqlPort}?prop1=value1")
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.size should be(1)
        connectionValues.config.get("prop1").get should be("value1")

        connectionValues = KsqlDriver.parseUrl(s"jdbc:ksql://${ksqlServer}:${ksqlPort}?prop1=value1&prop2=value2&prop3=value3")
        connectionValues.ksqlServer should be(ksqlServer)
        connectionValues.port should be(ksqlPort)
        connectionValues.config.size should be(3)
        connectionValues.config.get("prop1").get should be("value1")
        connectionValues.config.get("prop2").get should be("value2")
        connectionValues.config.get("prop3").get should be("value3")
      }
    }
  }

}
