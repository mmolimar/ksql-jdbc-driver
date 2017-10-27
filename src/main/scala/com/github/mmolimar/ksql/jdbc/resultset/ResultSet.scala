package com.github.mmolimar.ksql.jdbc.resultset

import java.io.{InputStream, Reader}
import java.math.BigDecimal
import java.net.URL
import java.sql.{Array, Blob, Clob, Date, NClob, Ref, ResultSet, ResultSetMetaData, RowId, SQLWarning, SQLXML, Statement, Time, Timestamp}
import java.util
import java.util.Calendar

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc.{EmptyRow, InvalidColumn, NotSupported, WrapperNotSupported}


private[resultset] class ResultSetNotSupported extends ResultSet with WrapperNotSupported {

  override def getType: Int = throw NotSupported()

  override def isBeforeFirst: Boolean = throw NotSupported()

  override def next: Boolean = throw NotSupported()

  override def updateString(columnIndex: Int, x: String): Unit = throw NotSupported()

  override def updateString(columnLabel: String, x: String): Unit = throw NotSupported()

  override def getTimestamp(columnIndex: Int): Timestamp = throw NotSupported()

  override def getTimestamp(columnLabel: String): Timestamp = throw NotSupported()

  override def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = throw NotSupported()

  override def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = throw NotSupported()

  override def updateNString(columnIndex: Int, nString: String): Unit = throw NotSupported()

  override def updateNString(columnLabel: String, nString: String): Unit = throw NotSupported()

  override def clearWarnings(): Unit = throw NotSupported()

  override def updateTimestamp(columnIndex: Int, x: Timestamp): Unit = throw NotSupported()

  override def updateTimestamp(columnLabel: String, x: Timestamp): Unit = throw NotSupported()

  override def updateByte(columnIndex: Int, x: Byte): Unit = throw NotSupported()

  override def updateByte(columnLabel: String, x: Byte): Unit = throw NotSupported()

  override def updateBigDecimal(columnIndex: Int, x: BigDecimal): Unit = throw NotSupported()

  override def updateBigDecimal(columnLabel: String, x: BigDecimal): Unit = throw NotSupported()

  override def updateDouble(columnIndex: Int, x: Double): Unit = throw NotSupported()

  override def updateDouble(columnLabel: String, x: Double): Unit = throw NotSupported()

  override def updateDate(columnIndex: Int, x: Date): Unit = throw NotSupported()

  override def updateDate(columnLabel: String, x: Date): Unit = throw NotSupported()

  override def isAfterLast: Boolean = throw NotSupported()

  override def updateBoolean(columnIndex: Int, x: Boolean): Unit = throw NotSupported()

  override def updateBoolean(columnLabel: String, x: Boolean): Unit = throw NotSupported()

  override def getBinaryStream(columnIndex: Int): InputStream = throw NotSupported()

  override def getBinaryStream(columnLabel: String): InputStream = throw NotSupported()

  override def beforeFirst(): Unit = throw NotSupported()

  override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = throw NotSupported()

  override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = throw NotSupported()

  override def updateNCharacterStream(columnIndex: Int, x: Reader): Unit = throw NotSupported()

  override def updateNCharacterStream(columnLabel: String, reader: Reader): Unit = throw NotSupported()

  override def updateNClob(columnIndex: Int, nClob: NClob): Unit = throw NotSupported()

  override def updateNClob(columnLabel: String, nClob: NClob): Unit = throw NotSupported()

  override def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit = throw NotSupported()

  override def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit = throw NotSupported()

  override def updateNClob(columnIndex: Int, reader: Reader): Unit = throw NotSupported()

  override def updateNClob(columnLabel: String, reader: Reader): Unit = throw NotSupported()

  override def last(): Boolean = throw NotSupported()

  override def isLast: Boolean = throw NotSupported()

  override def getNClob(columnIndex: Int): NClob = throw NotSupported()

  override def getNClob(columnLabel: String): NClob = throw NotSupported()

  override def getCharacterStream(columnIndex: Int): Reader = throw NotSupported()

  override def getCharacterStream(columnLabel: String): Reader = throw NotSupported()

  override def updateArray(columnIndex: Int, x: Array): Unit = throw NotSupported()

  override def updateArray(columnLabel: String, x: Array): Unit = throw NotSupported()

  override def updateBlob(columnIndex: Int, x: Blob): Unit = throw NotSupported()

  override def updateBlob(columnLabel: String, x: Blob): Unit = throw NotSupported()

  override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit = throw NotSupported()

  override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit = throw NotSupported()

  override def updateBlob(columnIndex: Int, inputStream: InputStream): Unit = throw NotSupported()

  override def updateBlob(columnLabel: String, inputStream: InputStream): Unit = throw NotSupported()

  override def getDouble(columnIndex: Int): Double = throw NotSupported()

  override def getDouble(columnLabel: String): Double = throw NotSupported()

  override def getArray(columnIndex: Int): Array = throw NotSupported()

  override def getArray(columnLabel: String): Array = throw NotSupported()

  override def isFirst: Boolean = throw NotSupported()

  override def getURL(columnIndex: Int): URL = throw NotSupported()

  override def getURL(columnLabel: String): URL = throw NotSupported()

  override def updateRow(): Unit = throw NotSupported()

  override def insertRow(): Unit = throw NotSupported()

  override def getMetaData: ResultSetMetaData = throw NotSupported()

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit = throw NotSupported()

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit = throw NotSupported()

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit = throw NotSupported()

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit = throw NotSupported()

  override def updateBinaryStream(columnIndex: Int, x: InputStream): Unit = throw NotSupported()

  override def updateBinaryStream(columnLabel: String, x: InputStream): Unit = throw NotSupported()

  override def absolute(row: Int): Boolean = throw NotSupported()

  override def updateRowId(columnIndex: Int, x: RowId): Unit = throw NotSupported()

  override def updateRowId(columnLabel: String, x: RowId): Unit = throw NotSupported()

  override def getRowId(columnIndex: Int): RowId = throw NotSupported()

  override def getRowId(columnLabel: String): RowId = throw NotSupported()

  override def moveToInsertRow(): Unit = throw NotSupported()

  override def rowInserted(): Boolean = throw NotSupported()

  override def getFloat(columnIndex: Int): Float = throw NotSupported()

  override def getFloat(columnLabel: String): Float = throw NotSupported()

  override def getBigDecimal(columnIndex: Int, scale: Int): BigDecimal = throw NotSupported()

  override def getBigDecimal(columnLabel: String, scale: Int): BigDecimal = throw NotSupported()

  override def getBigDecimal(columnIndex: Int): BigDecimal = throw NotSupported()

  override def getBigDecimal(columnLabel: String): BigDecimal = throw NotSupported()

  override def getClob(columnIndex: Int): Clob = throw NotSupported()

  override def getClob(columnLabel: String): Clob = throw NotSupported()

  override def getRow: Int = throw NotSupported()

  override def getLong(columnIndex: Int): Long = throw NotSupported()

  override def getLong(columnLabel: String): Long = throw NotSupported()

  override def getHoldability: Int = throw NotSupported()

  override def updateFloat(columnIndex: Int, x: Float): Unit = throw NotSupported()

  override def updateFloat(columnLabel: String, x: Float): Unit = throw NotSupported()

  override def afterLast(): Unit = throw NotSupported()

  override def refreshRow(): Unit = throw NotSupported()

  override def getNString(columnIndex: Int): String = throw NotSupported()

  override def getNString(columnLabel: String): String = throw NotSupported()

  override def deleteRow(): Unit = throw NotSupported()

  override def getConcurrency: Int = throw NotSupported()

  override def updateObject(columnIndex: Int, x: scala.Any, scaleOrLength: Int): Unit = throw NotSupported()

  override def updateObject(columnIndex: Int, x: scala.Any): Unit = throw NotSupported()

  override def updateObject(columnLabel: String, x: scala.Any, scaleOrLength: Int): Unit = throw NotSupported()

  override def updateObject(columnLabel: String, x: scala.Any): Unit = throw NotSupported()

  override def getFetchSize: Int = throw NotSupported()

  override def getTime(columnIndex: Int): Time = throw NotSupported()

  override def getTime(columnLabel: String): Time = throw NotSupported()

  override def getTime(columnIndex: Int, cal: Calendar): Time = throw NotSupported()

  override def getTime(columnLabel: String, cal: Calendar): Time = throw NotSupported()

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit = throw NotSupported()

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Int): Unit = throw NotSupported()

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = throw NotSupported()

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = throw NotSupported()

  override def updateCharacterStream(columnIndex: Int, x: Reader): Unit = throw NotSupported()

  override def updateCharacterStream(columnLabel: String, reader: Reader): Unit = throw NotSupported()

  override def getByte(columnIndex: Int): Byte = throw NotSupported()

  override def getByte(columnLabel: String): Byte = throw NotSupported()

  override def getBoolean(columnIndex: Int): Boolean = throw NotSupported()

  override def getBoolean(columnLabel: String): Boolean = throw NotSupported()

  override def setFetchDirection(direction: Int): Unit = throw NotSupported()

  override def getFetchDirection: Int = throw NotSupported()

  override def updateRef(columnIndex: Int, x: Ref): Unit = throw NotSupported()

  override def updateRef(columnLabel: String, x: Ref): Unit = throw NotSupported()

  override def getAsciiStream(columnIndex: Int): InputStream = throw NotSupported()

  override def getAsciiStream(columnLabel: String): InputStream = throw NotSupported()

  override def getShort(columnIndex: Int): Short = throw NotSupported()

  override def getShort(columnLabel: String): Short = throw NotSupported()

  override def getObject(columnIndex: Int): AnyRef = throw NotSupported()

  override def getObject(columnLabel: String): AnyRef = throw NotSupported()

  override def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef = throw NotSupported()

  override def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef = throw NotSupported()

  override def getObject[T](columnIndex: Int, `type`: Class[T]): T = throw NotSupported()

  override def getObject[T](columnLabel: String, `type`: Class[T]): T = throw NotSupported()

  override def updateShort(columnIndex: Int, x: Short): Unit = throw NotSupported()

  override def updateShort(columnLabel: String, x: Short): Unit = throw NotSupported()

  override def getNCharacterStream(columnIndex: Int): Reader = throw NotSupported()

  override def getNCharacterStream(columnLabel: String): Reader = throw NotSupported()

  override def close(): Unit = throw NotSupported()

  override def relative(rows: Int): Boolean = throw NotSupported()

  override def updateInt(columnIndex: Int, x: Int): Unit = throw NotSupported()

  override def updateInt(columnLabel: String, x: Int): Unit = throw NotSupported()

  override def wasNull(): Boolean = throw NotSupported()

  override def rowUpdated(): Boolean = throw NotSupported()

  override def getRef(columnIndex: Int): Ref = throw NotSupported()

  override def getRef(columnLabel: String): Ref = throw NotSupported()

  override def updateLong(columnIndex: Int, x: Long): Unit = throw NotSupported()

  override def updateLong(columnLabel: String, x: Long): Unit = throw NotSupported()

  override def moveToCurrentRow(): Unit = throw NotSupported()

  override def isClosed: Boolean = throw NotSupported()

  override def updateClob(columnIndex: Int, x: Clob): Unit = throw NotSupported()

  override def updateClob(columnLabel: String, x: Clob): Unit = throw NotSupported()

  override def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit = throw NotSupported()

  override def updateClob(columnLabel: String, reader: Reader, length: Long): Unit = throw NotSupported()

  override def updateClob(columnIndex: Int, reader: Reader): Unit = throw NotSupported()

  override def updateClob(columnLabel: String, reader: Reader): Unit = throw NotSupported()

  override def findColumn(columnLabel: String): Int = throw NotSupported()

  override def getWarnings: SQLWarning = throw NotSupported()

  override def getDate(columnIndex: Int): Date = throw NotSupported()

  override def getDate(columnLabel: String): Date = throw NotSupported()

  override def getDate(columnIndex: Int, cal: Calendar): Date = throw NotSupported()

  override def getDate(columnLabel: String, cal: Calendar): Date = throw NotSupported()

  override def getCursorName: String = throw NotSupported()

  override def updateNull(columnIndex: Int): Unit = throw NotSupported()

  override def updateNull(columnLabel: String): Unit = throw NotSupported()

  override def getStatement: Statement = throw NotSupported()

  override def cancelRowUpdates(): Unit = throw NotSupported()

  override def getSQLXML(columnIndex: Int): SQLXML = throw NotSupported()

  override def getSQLXML(columnLabel: String): SQLXML = throw NotSupported()

  override def getUnicodeStream(columnIndex: Int): InputStream = throw NotSupported()

  override def getUnicodeStream(columnLabel: String): InputStream = throw NotSupported()

  override def getInt(columnIndex: Int): Int = throw NotSupported()

  override def getInt(columnLabel: String): Int = throw NotSupported()

  override def updateTime(columnIndex: Int, x: Time): Unit = throw NotSupported()

  override def updateTime(columnLabel: String, x: Time): Unit = throw NotSupported()

  override def setFetchSize(rows: Int): Unit = throw NotSupported()

  override def previous(): Boolean = throw NotSupported()

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit = throw NotSupported()

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit = throw NotSupported()

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit = throw NotSupported()

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit = throw NotSupported()

  override def updateAsciiStream(columnIndex: Int, x: InputStream): Unit = throw NotSupported()

  override def updateAsciiStream(columnLabel: String, x: InputStream): Unit = throw NotSupported()

  override def rowDeleted(): Boolean = throw NotSupported()

  override def getBlob(columnIndex: Int): Blob = throw NotSupported()

  override def getBlob(columnLabel: String): Blob = throw NotSupported()

  override def first(): Boolean = throw NotSupported()

  override def getBytes(columnIndex: Int): scala.Array[Byte] = throw NotSupported()

  override def getBytes(columnLabel: String): scala.Array[Byte] = throw NotSupported()

  override def updateBytes(columnIndex: Int, x: scala.Array[Byte]): Unit = throw NotSupported()

  override def updateBytes(columnLabel: String, x: scala.Array[Byte]): Unit = throw NotSupported()

  override def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit = throw NotSupported()

  override def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit = throw NotSupported()

  override def getString(columnIndex: Int): String = throw NotSupported()

  override def getString(columnLabel: String): String = throw NotSupported()

}

abstract class AbstractResultSet[T](private val iterator: Iterator[T]) extends ResultSetNotSupported {

  protected var currentRow: Option[T] = None

  override def next: Boolean = iterator.hasNext match {
    case true =>
      currentRow = Some(iterator.next)
      true
    case false => false
  }

  override def getBoolean(columnIndex: Int): Boolean = getColumn(columnIndex)

  override def getBoolean(columnLabel: String): Boolean = getColumn(columnLabel)

  override def getByte(columnIndex: Int): Byte = getColumn(columnIndex)

  override def getByte(columnLabel: String): Byte = getColumn(columnLabel)

  override def getShort(columnIndex: Int): Short = getColumn(columnIndex)

  override def getShort(columnLabel: String): Short = getColumn(columnLabel)

  override def getInt(columnIndex: Int): Int = getColumn(columnIndex)

  override def getInt(columnLabel: String): Int = getColumn(columnLabel)

  override def getLong(columnIndex: Int): Long = getColumn(columnIndex)

  override def getLong(columnLabel: String): Long = getColumn(columnLabel)

  override def getFloat(columnIndex: Int): Float = getColumn(columnIndex)

  override def getFloat(columnLabel: String): Float = getColumn(columnLabel)

  override def getDouble(columnIndex: Int): Double = getColumn(columnIndex)

  override def getDouble(columnLabel: String): Double = getColumn(columnLabel)

  override def getBytes(columnIndex: Int): scala.Array[Byte] = getColumn(columnIndex)

  override def getBytes(columnLabel: String): scala.Array[Byte] = getColumn(columnLabel)

  override def getString(columnIndex: Int): String = getColumn(columnIndex)

  override def getString(columnLabel: String): String = getColumn(columnLabel)

  private def getColumn[T <: AnyRef](columnLabel: String): T = getColumn(getColumnIndex(columnLabel))

  private def getColumn[T <: AnyRef](columnIndex: Int): T = {
    checkRow(columnIndex)
    getValue(columnIndex)
  }

  private def checkRow(columnIndex: Int) = {
    def checkIfEmpty = if (isEmpty) throw EmptyRow()

    def checkColumnBounds(columnIndex: Int) = {
      val (min, max) = getColumnBounds
      if (columnIndex < min || columnIndex > max)
        throw InvalidColumn(s"Column with index ${columnIndex} does not exist")
    }

    checkIfEmpty
    checkColumnBounds(columnIndex)
  }

  protected def isEmpty: Boolean = currentRow.isEmpty

  protected def getColumnBounds: (Int, Int)

  protected def getValue[T <: AnyRef](columnIndex: Int): T

  protected def getColumnIndex(columnLabel: String): Int
}
