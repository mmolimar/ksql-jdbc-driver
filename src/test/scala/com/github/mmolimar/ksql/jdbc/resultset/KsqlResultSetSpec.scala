package com.github.mmolimar.ksql.jdbc.resultset

import java.sql._

import com.github.mmolimar.ksql.jdbc.utils.TestUtils._
import com.github.mmolimar.ksql.jdbc.{DatabaseMetadataHeaders, HeaderField, TableTypes}
import io.confluent.ksql.GenericRow
import io.confluent.ksql.rest.entity.StreamedRow
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.collection.JavaConverters._


class KsqlResultSetSpec extends WordSpec with Matchers with MockFactory with OneInstancePerTest {

  "A IteratorResultSet" when {

    "validating specs" should {

      "throw not supported exception if not supported" in {

        val resultSet = new IteratorResultSet[String](List.empty[HeaderField], 0, Iterator.empty)
        val methods = implementedMethods[IteratorResultSet[String]] ++ implementedMethods[AbstractResultSet[String]]
        reflectMethods[IteratorResultSet[String]](methods, false, resultSet)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }

      "work if implemented" in {

        val resultSet = new IteratorResultSet(DatabaseMetadataHeaders.tableTypes, 2, Iterator(Seq(TableTypes.TABLE.name),
          Seq(TableTypes.STREAM.name)))

        resultSet.wasNull should be(true)
        resultSet.next should be(true)

        resultSet.getString(1) should be(TableTypes.TABLE.name)
        resultSet.getString("TABLE_TYPE") should be(TableTypes.TABLE.name)
        resultSet.getString("table_type") should be(TableTypes.TABLE.name)
        resultSet.next should be(true)
        resultSet.getString(1) should be(TableTypes.STREAM.name)
        resultSet.getString("TABLE_TYPE") should be(TableTypes.STREAM.name)
        resultSet.getString("table_type") should be(TableTypes.STREAM.name)
        resultSet.wasNull should be(false)
        assertThrows[SQLException] {
          resultSet.getString("UNKNOWN")
        }
        resultSet.next should be(false)
        resultSet.getWarnings should be(None.orNull)
        resultSet.close
      }
    }
  }

  "A StreamedResultSet" when {

    "validating specs" should {

      val resultSetMetadata = new KsqlResultSetMetaData(
        List(
          HeaderField("field1", Types.INTEGER, 16),
          HeaderField("field2", Types.BIGINT, 16),
          HeaderField("field3", Types.DOUBLE, 16),
          HeaderField("field4", Types.BOOLEAN, 16),
          HeaderField("field5", Types.VARCHAR, 16),
          HeaderField("field6", Types.JAVA_OBJECT, 16),
          HeaderField("field7", Types.ARRAY, 16),
          HeaderField("field8", Types.STRUCT, 16),
          HeaderField("field9", -999, 16)
        ))

      "throw not supported exception if not supported" in {

        val resultSet = new StreamedResultSet(resultSetMetadata, mock[KsqlQueryStream], 0)
        val methods = implementedMethods[StreamedResultSet] ++ implementedMethods[AbstractResultSet[StreamedRow]]
        reflectMethods[StreamedResultSet](methods, false, resultSet)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              try {
                method()
                println("")
              } catch {
                case e: Throwable => throw e
              }
            }
          })
      }

      "work when reading from a query stream" in {

        val mockedQueryStream = mock[KsqlQueryStream]
        inSequence {
          (mockedQueryStream.hasNext _).expects.returns(true)
          (mockedQueryStream.hasNext _).expects.returns(true)
          val columnValues = Seq[AnyRef](Int.box(1), Long.box(2L), Double.box(3.3d), Boolean.box(true),
            "1", Map.empty, scala.Array.empty, Map.empty, None.orNull)
          val row = StreamedRow.row(new GenericRow(columnValues.asJava))
          (mockedQueryStream.next _).expects.returns(row)
          (mockedQueryStream.hasNext _).expects.returns(false)
          (mockedQueryStream.close _).expects
        }

        val resultSet = new StreamedResultSet(resultSetMetadata, mockedQueryStream, 0)
        resultSet.getMetaData should be(resultSetMetadata)
        resultSet.isLast should be(false)
        resultSet.isAfterLast should be(false)
        resultSet.isBeforeFirst should be(false)
        resultSet.getConcurrency should be(ResultSet.CONCUR_READ_ONLY)
        resultSet.wasNull should be(true)

        resultSet.isFirst should be(true)
        resultSet.next should be(true)

        // just to validate proper maps in data types
        val expected = Seq(
          Seq("1", scala.Array(1.byteValue), Boolean.box(true), Byte.box(1),
            Short.box(1), Int.box(1), Long.box(1L), Float.box(1.0f), Double.box(1.0d)),
          Seq("2", scala.Array(2L.byteValue), Boolean.box(true), Byte.box(2),
            Short.box(2), Int.box(2), Long.box(2L), Float.box(2.0f), Double.box(2.0d)),
          Seq("3.3", scala.Array(3L.byteValue), Boolean.box(true), Byte.box(3),
            Short.box(3), Int.box(3), Long.box(3L), Float.box(3.3f), Double.box(3.3d)),
          Seq("true", scala.Array(1.byteValue), Boolean.box(true), Byte.box(1),
            Short.box(1), Int.box(1), Long.box(1L), Float.box(1.0f), Double.box(1.0d)),
          Seq("1", "1".getBytes, Boolean.box(false), Byte.box(1),
            Short.box(1), Int.box(1), Long.box(1L), Float.box(1.0f), Double.box(1.0d))
        )
        expected.zipWithIndex.map { case (e, index) => {
          resultSet.getString(index + 1) should be(e(0))
          resultSet.getBytes(index + 1) should be(e(1))
          resultSet.getBoolean(index + 1) should be(e(2))
          resultSet.getByte(index + 1) should be(e(3))
          resultSet.getShort(index + 1) should be(e(4))
          resultSet.getInt(index + 1) should be(e(5))
          resultSet.getLong(index + 1) should be(e(6))
          resultSet.getFloat(index + 1) should be(e(7))
          resultSet.getDouble(index + 1) should be(e(8))
          resultSet.wasNull should be(false)
        }
        }

        assertThrows[SQLException] {
          resultSet.getString(1000)
        }
        assertThrows[SQLException] {
          resultSet.getString("UNKNOWN")
        }
        assertThrows[SQLException] {
          resultSet.getBytes("UNKNOWN")
        }
        assertThrows[SQLException] {
          resultSet.getBoolean("UNKNOWN")
        }
        assertThrows[SQLException] {
          resultSet.getByte("UNKNOWN")
        }
        assertThrows[SQLException] {
          resultSet.getShort("UNKNOWN")
        }
        assertThrows[SQLException] {
          resultSet.getInt("UNKNOWN")
        }
        assertThrows[SQLException] {
          resultSet.getLong("UNKNOWN")
        }
        assertThrows[SQLException] {
          resultSet.getFloat("UNKNOWN")
        }
        assertThrows[SQLException] {
          resultSet.getDouble("UNKNOWN")
        }

        resultSet.next should be(false)
        resultSet.isFirst should be(false)
        resultSet.getWarnings should be(None.orNull)
        resultSet.close
        resultSet.close
        assertThrows[SQLException] {
          resultSet.next
        }
      }

      "work when reading from an input stream" in {

        val mockedInputStream = mock[KsqlInputStream]
        inSequence {
          (mockedInputStream.hasNext _).expects.returns(true)
          (mockedInputStream.hasNext _).expects.returns(true)
          val columnValues = Seq[AnyRef]("test")
          val row = StreamedRow.row(new GenericRow(columnValues.asJava))
          (mockedInputStream.next _).expects.returns(row)
          (mockedInputStream.hasNext _).expects.returns(false)
          (mockedInputStream.close _).expects
        }

        val resultSet = new StreamedResultSet(resultSetMetadata, mockedInputStream, 0)
        resultSet.getMetaData should be(resultSetMetadata)
        resultSet.isLast should be(false)
        resultSet.isAfterLast should be(false)
        resultSet.isBeforeFirst should be(false)
        resultSet.getConcurrency should be(ResultSet.CONCUR_READ_ONLY)
        resultSet.wasNull should be(true)

        resultSet.isFirst should be(true)
        resultSet.next should be(true)

        resultSet.getString(1) should be("test")
        resultSet.next should be(false)
        resultSet.isFirst should be(false)
        resultSet.getWarnings should be(None.orNull)
        resultSet.close
        resultSet.close
        assertThrows[SQLException] {
          resultSet.next
        }
      }
    }
  }

  "A ResultSetNotSupported" when {

    "validating specs" should {

      "throw not supported exception if not supported" in {

        val resultSet = new ResultSetNotSupported
        reflectMethods[ResultSetNotSupported](Seq.empty, false, resultSet)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              method()
            }
          })
      }
    }
  }

}
