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
        driver.acceptsURL("jdbc:invalid://broker1:9999") shouldBe (false)
      }
      "return false if valid" in {
        driver.acceptsURL("jdbc:ksql://broker1:9999") shouldBe (true)
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
          KsqlDriver.parseUrl("jdbc:invalid://broker1:9999")
        }
      }
      "return the URL parsed properly" in {

      }
    }

  }

}
