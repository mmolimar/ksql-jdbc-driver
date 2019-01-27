package com.github.mmolimar.ksql.jdbc.resultset

import java.io.{InputStream, Reader}
import java.math.BigDecimal
import java.net.URL
import java.sql.{Array, Blob, Clob, Date, NClob, Ref, ResultSet, ResultSetMetaData, RowId, SQLWarning, SQLXML, Statement, Time, Timestamp}
import java.util
import java.util.Calendar

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc._


private[resultset] class ResultSetNotSupported extends ResultSet with WrapperNotSupported {

  override def getType: Int = throw NotSupported("getType")

  override def isBeforeFirst: Boolean = throw NotSupported("isBeforeFirst")

  override def next: Boolean = throw NotSupported("next")

  override def updateString(columnIndex: Int, x: String): Unit = throw NotSupported("updateString")

  override def updateString(columnLabel: String, x: String): Unit = throw NotSupported("updateString")

  override def getTimestamp(columnIndex: Int): Timestamp = throw NotSupported("getTimestamp")

  override def getTimestamp(columnLabel: String): Timestamp = throw NotSupported("getTimestamp")

  override def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = throw NotSupported("getTimestamp")

  override def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = throw NotSupported("getTimestamp")

  override def updateNString(columnIndex: Int, nString: String): Unit = throw NotSupported("updateNString")

  override def updateNString(columnLabel: String, nString: String): Unit = throw NotSupported("updateNString")

  override def clearWarnings(): Unit = throw NotSupported("clearWarnings")

  override def updateTimestamp(columnIndex: Int, x: Timestamp): Unit = throw NotSupported("updateTimestamp")

  override def updateTimestamp(columnLabel: String, x: Timestamp): Unit = throw NotSupported("updateTimestamp")

  override def updateByte(columnIndex: Int, x: Byte): Unit = throw NotSupported("updateByte")

  override def updateByte(columnLabel: String, x: Byte): Unit = throw NotSupported("updateByte")

  override def updateBigDecimal(columnIndex: Int, x: BigDecimal): Unit = throw NotSupported("updateBigDecimal")

  override def updateBigDecimal(columnLabel: String, x: BigDecimal): Unit = throw NotSupported("updateBigDecimal")

  override def updateDouble(columnIndex: Int, x: Double): Unit = throw NotSupported("updateDouble")

  override def updateDouble(columnLabel: String, x: Double): Unit = throw NotSupported("updateDouble")

  override def updateDate(columnIndex: Int, x: Date): Unit = throw NotSupported("updateDate")

  override def updateDate(columnLabel: String, x: Date): Unit = throw NotSupported("updateDate")

  override def isAfterLast: Boolean = throw NotSupported("isAfterLast")

  override def updateBoolean(columnIndex: Int, x: Boolean): Unit = throw NotSupported("updateBoolean")

  override def updateBoolean(columnLabel: String, x: Boolean): Unit = throw NotSupported("updateBoolean")

  override def getBinaryStream(columnIndex: Int): InputStream = throw NotSupported("getBinaryStream")

  override def getBinaryStream(columnLabel: String): InputStream = throw NotSupported("getBinaryStream")

  override def beforeFirst(): Unit = throw NotSupported("beforeFirst")

  override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit =
    throw NotSupported("updateNCharacterStream")

  override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit =
    throw NotSupported("updateNCharacterStream")

  override def updateNCharacterStream(columnIndex: Int, x: Reader): Unit =
    throw NotSupported("updateNCharacterStream")

  override def updateNCharacterStream(columnLabel: String, reader: Reader): Unit =
    throw NotSupported("updateNCharacterStream")

  override def updateNClob(columnIndex: Int, nClob: NClob): Unit = throw NotSupported("updateNClob")

  override def updateNClob(columnLabel: String, nClob: NClob): Unit = throw NotSupported("updateNClob")

  override def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit = throw NotSupported("updateNClob")

  override def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit = throw NotSupported("updateNClob")

  override def updateNClob(columnIndex: Int, reader: Reader): Unit = throw NotSupported("updateNClob")

  override def updateNClob(columnLabel: String, reader: Reader): Unit = throw NotSupported("updateNClob")

  override def last(): Boolean = throw NotSupported("last")

  override def isLast: Boolean = throw NotSupported("isLast")

  override def getNClob(columnIndex: Int): NClob = throw NotSupported("getNClob")

  override def getNClob(columnLabel: String): NClob = throw NotSupported("getNClob")

  override def getCharacterStream(columnIndex: Int): Reader = throw NotSupported("getCharacterStream")

  override def getCharacterStream(columnLabel: String): Reader = throw NotSupported("getCharacterStream")

  override def updateArray(columnIndex: Int, x: Array): Unit = throw NotSupported("updateArray")

  override def updateArray(columnLabel: String, x: Array): Unit = throw NotSupported("updateArray")

  override def updateBlob(columnIndex: Int, x: Blob): Unit = throw NotSupported("updateBlob")

  override def updateBlob(columnLabel: String, x: Blob): Unit = throw NotSupported("updateBlob")

  override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit = throw NotSupported("updateBlob")

  override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit = throw NotSupported("updateBlob")

  override def updateBlob(columnIndex: Int, inputStream: InputStream): Unit = throw NotSupported("updateBlob")

  override def updateBlob(columnLabel: String, inputStream: InputStream): Unit = throw NotSupported("updateBlob")

  override def getDouble(columnIndex: Int): Double = throw NotSupported("getDouble")

  override def getDouble(columnLabel: String): Double = throw NotSupported("getDouble")

  override def getArray(columnIndex: Int): Array = throw NotSupported("getArray")

  override def getArray(columnLabel: String): Array = throw NotSupported("getArray")

  override def isFirst: Boolean = throw NotSupported("isFirst")

  override def getURL(columnIndex: Int): URL = throw NotSupported("getURL")

  override def getURL(columnLabel: String): URL = throw NotSupported("getURL")

  override def updateRow(): Unit = throw NotSupported("updateRow")

  override def insertRow(): Unit = throw NotSupported("insertRow")

  override def getMetaData: ResultSetMetaData = throw NotSupported("getMetaData")

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit = throw NotSupported("updateBinaryStream")

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit = throw NotSupported("updateBinaryStream")

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit = throw NotSupported("updateBinaryStream")

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit = throw NotSupported("updateBinaryStream")

  override def updateBinaryStream(columnIndex: Int, x: InputStream): Unit = throw NotSupported("updateBinaryStream")

  override def updateBinaryStream(columnLabel: String, x: InputStream): Unit = throw NotSupported("updateBinaryStream")

  override def absolute(row: Int): Boolean = throw NotSupported("absolute")

  override def updateRowId(columnIndex: Int, x: RowId): Unit = throw NotSupported("updateRowId")

  override def updateRowId(columnLabel: String, x: RowId): Unit = throw NotSupported("updateRowId")

  override def getRowId(columnIndex: Int): RowId = throw NotSupported("getRowId")

  override def getRowId(columnLabel: String): RowId = throw NotSupported("getRowId")

  override def moveToInsertRow(): Unit = throw NotSupported("moveToInsertRow")

  override def rowInserted(): Boolean = throw NotSupported("rowInserted")

  override def getFloat(columnIndex: Int): Float = throw NotSupported("getFloat")

  override def getFloat(columnLabel: String): Float = throw NotSupported("getFloat")

  override def getBigDecimal(columnIndex: Int, scale: Int): BigDecimal = throw NotSupported("getBigDecimal")

  override def getBigDecimal(columnLabel: String, scale: Int): BigDecimal = throw NotSupported("getBigDecimal")

  override def getBigDecimal(columnIndex: Int): BigDecimal = throw NotSupported("getBigDecimal")

  override def getBigDecimal(columnLabel: String): BigDecimal = throw NotSupported("getBigDecimal")

  override def getClob(columnIndex: Int): Clob = throw NotSupported("getClob")

  override def getClob(columnLabel: String): Clob = throw NotSupported("getClob")

  override def getRow: Int = throw NotSupported("getRow")

  override def getLong(columnIndex: Int): Long = throw NotSupported("getLong")

  override def getLong(columnLabel: String): Long = throw NotSupported("getLong")

  override def getHoldability: Int = throw NotSupported("getHoldability")

  override def updateFloat(columnIndex: Int, x: Float): Unit = throw NotSupported("updateFloat")

  override def updateFloat(columnLabel: String, x: Float): Unit = throw NotSupported("updateFloat")

  override def afterLast: Unit = throw NotSupported("afterLast")

  override def refreshRow: Unit = throw NotSupported("refreshRow")

  override def getNString(columnIndex: Int): String = throw NotSupported("getNString")

  override def getNString(columnLabel: String): String = throw NotSupported("getNString")

  override def deleteRow: Unit = throw NotSupported("deleteRow")

  override def getConcurrency: Int = throw NotSupported("getConcurrency")

  override def updateObject(columnIndex: Int, x: scala.Any, scaleOrLength: Int): Unit = throw NotSupported("updateObject")

  override def updateObject(columnIndex: Int, x: scala.Any): Unit = throw NotSupported("updateObject")

  override def updateObject(columnLabel: String, x: scala.Any, scaleOrLength: Int): Unit = throw NotSupported("updateObject")

  override def updateObject(columnLabel: String, x: scala.Any): Unit = throw NotSupported("updateObject")

  override def getFetchSize: Int = throw NotSupported("getFetchSize")

  override def getTime(columnIndex: Int): Time = throw NotSupported("getTime")

  override def getTime(columnLabel: String): Time = throw NotSupported("getTime")

  override def getTime(columnIndex: Int, cal: Calendar): Time = throw NotSupported("getTime")

  override def getTime(columnLabel: String, cal: Calendar): Time = throw NotSupported("getTime")

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit = throw NotSupported("updateCharacterStream")

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Int): Unit = throw NotSupported("updateCharacterStream")

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = throw NotSupported("updateCharacterStream")

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = throw NotSupported("updateCharacterStream")

  override def updateCharacterStream(columnIndex: Int, x: Reader): Unit = throw NotSupported("updateCharacterStream")

  override def updateCharacterStream(columnLabel: String, reader: Reader): Unit = throw NotSupported("updateCharacterStream")

  override def getByte(columnIndex: Int): Byte = throw NotSupported("getByte")

  override def getByte(columnLabel: String): Byte = throw NotSupported("getByte")

  override def getBoolean(columnIndex: Int): Boolean = throw NotSupported("getBoolean")

  override def getBoolean(columnLabel: String): Boolean = throw NotSupported("getBoolean")

  override def setFetchDirection(direction: Int): Unit = throw NotSupported("setFetchDirection")

  override def getFetchDirection: Int = throw NotSupported("getFetchDirection")

  override def updateRef(columnIndex: Int, x: Ref): Unit = throw NotSupported("updateRef")

  override def updateRef(columnLabel: String, x: Ref): Unit = throw NotSupported("updateRef")

  override def getAsciiStream(columnIndex: Int): InputStream = throw NotSupported("getAsciiStream")

  override def getAsciiStream(columnLabel: String): InputStream = throw NotSupported("getAsciiStream")

  override def getShort(columnIndex: Int): Short = throw NotSupported("getShort")

  override def getShort(columnLabel: String): Short = throw NotSupported("getShort")

  override def getObject(columnIndex: Int): AnyRef = throw NotSupported("getObject")

  override def getObject(columnLabel: String): AnyRef = throw NotSupported("getObject")

  override def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef = throw NotSupported("getObject")

  override def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef = throw NotSupported("getObject")

  override def getObject[T](columnIndex: Int, `type`: Class[T]): T = throw NotSupported("getObject")

  override def getObject[T](columnLabel: String, `type`: Class[T]): T = throw NotSupported("getObject")

  override def updateShort(columnIndex: Int, x: Short): Unit = throw NotSupported("updateShort")

  override def updateShort(columnLabel: String, x: Short): Unit = throw NotSupported("updateShort")

  override def getNCharacterStream(columnIndex: Int): Reader = throw NotSupported("getNCharacterStream")

  override def getNCharacterStream(columnLabel: String): Reader = throw NotSupported("getNCharacterStream")

  override def close: Unit = throw NotSupported("close")

  override def relative(rows: Int): Boolean = throw NotSupported("relative")

  override def updateInt(columnIndex: Int, x: Int): Unit = throw NotSupported("updateInt")

  override def updateInt(columnLabel: String, x: Int): Unit = throw NotSupported("updateInt")

  override def wasNull: Boolean = throw NotSupported("wasNull")

  override def rowUpdated: Boolean = throw NotSupported("rowUpdated")

  override def getRef(columnIndex: Int): Ref = throw NotSupported("getRef")

  override def getRef(columnLabel: String): Ref = throw NotSupported("getRef")

  override def updateLong(columnIndex: Int, x: Long): Unit = throw NotSupported("updateLong")

  override def updateLong(columnLabel: String, x: Long): Unit = throw NotSupported("updateLong")

  override def moveToCurrentRow: Unit = throw NotSupported("moveToCurrentRow")

  override def isClosed: Boolean = throw NotSupported("isClosed")

  override def updateClob(columnIndex: Int, x: Clob): Unit = throw NotSupported("updateClob")

  override def updateClob(columnLabel: String, x: Clob): Unit = throw NotSupported("updateClob")

  override def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit = throw NotSupported("updateClob")

  override def updateClob(columnLabel: String, reader: Reader, length: Long): Unit = throw NotSupported("updateClob")

  override def updateClob(columnIndex: Int, reader: Reader): Unit = throw NotSupported("updateClob")

  override def updateClob(columnLabel: String, reader: Reader): Unit = throw NotSupported("updateClob")

  override def findColumn(columnLabel: String): Int = throw NotSupported("findColumn")

  override def getWarnings: SQLWarning = throw NotSupported("getWarnings")

  override def getDate(columnIndex: Int): Date = throw NotSupported("getDate")

  override def getDate(columnLabel: String): Date = throw NotSupported("getDate")

  override def getDate(columnIndex: Int, cal: Calendar): Date = throw NotSupported("getDate")

  override def getDate(columnLabel: String, cal: Calendar): Date = throw NotSupported("getDate")

  override def getCursorName: String = throw NotSupported("getCursorName")

  override def updateNull(columnIndex: Int): Unit = throw NotSupported("updateNull")

  override def updateNull(columnLabel: String): Unit = throw NotSupported("updateNull")

  override def getStatement: Statement = throw NotSupported("getStatement")

  override def cancelRowUpdates: Unit = throw NotSupported("cancelRowUpdates")

  override def getSQLXML(columnIndex: Int): SQLXML = throw NotSupported("getSQLXML")

  override def getSQLXML(columnLabel: String): SQLXML = throw NotSupported("getSQLXML")

  override def getUnicodeStream(columnIndex: Int): InputStream = throw NotSupported("getUnicodeStream")

  override def getUnicodeStream(columnLabel: String): InputStream = throw NotSupported("getUnicodeStream")

  override def getInt(columnIndex: Int): Int = throw NotSupported("getInt")

  override def getInt(columnLabel: String): Int = throw NotSupported("getInt")

  override def updateTime(columnIndex: Int, x: Time): Unit = throw NotSupported("updateTime")

  override def updateTime(columnLabel: String, x: Time): Unit = throw NotSupported("updateTime")

  override def setFetchSize(rows: Int): Unit = throw NotSupported("setFetchSize")

  override def previous: Boolean = throw NotSupported("previous")

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit = throw NotSupported("updateAsciiStream")

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit = throw NotSupported("updateAsciiStream")

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit = throw NotSupported("updateAsciiStream")

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit = throw NotSupported("updateAsciiStream")

  override def updateAsciiStream(columnIndex: Int, x: InputStream): Unit = throw NotSupported("updateAsciiStream")

  override def updateAsciiStream(columnLabel: String, x: InputStream): Unit = throw NotSupported("updateAsciiStream")

  override def rowDeleted: Boolean = throw NotSupported("rowDeleted")

  override def getBlob(columnIndex: Int): Blob = throw NotSupported("getBlob")

  override def getBlob(columnLabel: String): Blob = throw NotSupported("getBlob")

  override def first: Boolean = throw NotSupported("first")

  override def getBytes(columnIndex: Int): scala.Array[Byte] = throw NotSupported("getBytes")

  override def getBytes(columnLabel: String): scala.Array[Byte] = throw NotSupported("getBytes")

  override def updateBytes(columnIndex: Int, x: scala.Array[Byte]): Unit = throw NotSupported("updateBytes")

  override def updateBytes(columnLabel: String, x: scala.Array[Byte]): Unit = throw NotSupported("updateBytes")

  override def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit = throw NotSupported("updateSQLXML")

  override def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit = throw NotSupported("updateSQLXML")

  override def getString(columnIndex: Int): String = throw NotSupported("getString")

  override def getString(columnLabel: String): String = throw NotSupported("getString")

}

abstract class AbstractResultSet[T](private val iterator: Iterator[T]) extends ResultSetNotSupported {

  protected var currentRow: Option[T] = None

  private var closed: Boolean = false

  protected def nextResult: Boolean = iterator.hasNext match {
    case true =>
      currentRow = Some(iterator.next)
      true
    case false => false
  }

  override final def next: Boolean = closed match {
    case true => throw ResultSetError("Result set is already closed.")
    case false => nextResult
  }

  override final def close: Unit = closed match {
    case true => // do nothing
    case false =>
      currentRow = None
      closeInherit
      closed = true
  }

  protected def closeInherit: Unit = {}

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
