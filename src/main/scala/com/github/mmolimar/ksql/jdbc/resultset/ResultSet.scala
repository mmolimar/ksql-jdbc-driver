package com.github.mmolimar.ksql.jdbc.resultset

import java.io.{InputStream, Reader}
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.math.BigDecimal
import java.net.URL
import java.sql.{Array, Blob, Clob, Date, NClob, Ref, ResultSet, ResultSetMetaData, RowId, SQLWarning, SQLXML, Statement, Time, Timestamp}
import java.util
import java.util.Calendar

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc._

import scala.reflect.ClassTag


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

  override def last: Boolean = throw NotSupported("last")

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

  override def rowInserted: Boolean = throw NotSupported("rowInserted")

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

  override def afterLast(): Unit = throw NotSupported("afterLast")

  override def refreshRow(): Unit = throw NotSupported("refreshRow")

  override def getNString(columnIndex: Int): String = throw NotSupported("getNString")

  override def getNString(columnLabel: String): String = throw NotSupported("getNString")

  override def deleteRow(): Unit = throw NotSupported("deleteRow")

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

  override def close(): Unit = throw NotSupported("close")

  override def relative(rows: Int): Boolean = throw NotSupported("relative")

  override def updateInt(columnIndex: Int, x: Int): Unit = throw NotSupported("updateInt")

  override def updateInt(columnLabel: String, x: Int): Unit = throw NotSupported("updateInt")

  override def wasNull: Boolean = throw NotSupported("wasNull")

  override def rowUpdated: Boolean = throw NotSupported("rowUpdated")

  override def getRef(columnIndex: Int): Ref = throw NotSupported("getRef")

  override def getRef(columnLabel: String): Ref = throw NotSupported("getRef")

  override def updateLong(columnIndex: Int, x: Long): Unit = throw NotSupported("updateLong")

  override def updateLong(columnLabel: String, x: Long): Unit = throw NotSupported("updateLong")

  override def moveToCurrentRow(): Unit = throw NotSupported("moveToCurrentRow")

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

  override def cancelRowUpdates(): Unit = throw NotSupported("cancelRowUpdates")

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

private[resultset] abstract class AbstractResultSet[T](private val metadata: ResultSetMetaData,
                                                       private val maxRows: Long,
                                                       private val records: Iterator[T]) extends ResultSetNotSupported {

  private val indexByLabel: Map[String, Int] = (1 to metadata.getColumnCount)
    .map(index => metadata.getColumnLabel(index).toUpperCase -> index).toMap
  private var lastColumnNull = true
  private var rowCounter = 0

  protected var currentRow: Option[T] = None

  private var closed: Boolean = false

  def hasNext: Boolean = !closed && maxRows != 0 && rowCounter < maxRows && records.hasNext

  protected def nextResult: Boolean = records.hasNext match {
    case true =>
      currentRow = Some(records.next)
      true
    case false => false
  }

  override final def next: Boolean = closed match {
    case true => throw ResultSetError("Result set is already closed.")
    case false if maxRows != 0 && rowCounter > maxRows => false
    case _ =>
      val result = nextResult
      rowCounter += 1
      result
  }

  override final def close(): Unit = closed match {
    case true => // do nothing
    case false =>
      currentRow = None
      closeInherit()
      closed = true
  }

  protected def closeInherit(): Unit

  override def getBoolean(columnIndex: Int): Boolean = getColumn[JBoolean](columnIndex)

  override def getBoolean(columnLabel: String): Boolean = getColumn[JBoolean](columnLabel)

  override def getByte(columnIndex: Int): Byte = getColumn[JByte](columnIndex)

  override def getByte(columnLabel: String): Byte = getColumn[JByte](columnLabel)

  override def getShort(columnIndex: Int): Short = getColumn[JShort](columnIndex)

  override def getShort(columnLabel: String): Short = getColumn[JShort](columnLabel)

  override def getInt(columnIndex: Int): Int = getColumn[JInt](columnIndex)

  override def getInt(columnLabel: String): Int = getColumn[JInt](columnLabel)

  override def getLong(columnIndex: Int): Long = getColumn[JLong](columnIndex)

  override def getLong(columnLabel: String): Long = getColumn[JLong](columnLabel)

  override def getFloat(columnIndex: Int): Float = getColumn[JFloat](columnIndex)

  override def getFloat(columnLabel: String): Float = getColumn[JFloat](columnLabel)

  override def getDouble(columnIndex: Int): Double = getColumn[JDouble](columnIndex)

  override def getDouble(columnLabel: String): Double = getColumn[JDouble](columnLabel)

  override def getBytes(columnIndex: Int): scala.Array[Byte] = getColumn[scala.Array[Byte]](columnIndex)

  override def getBytes(columnLabel: String): scala.Array[Byte] = getColumn[scala.Array[Byte]](columnLabel)

  override def getString(columnIndex: Int): String = getColumn[String](columnIndex)

  override def getString(columnLabel: String): String = getColumn[String](columnLabel)

  override def getObject(columnIndex: Int): AnyRef = getColumn[AnyRef](columnIndex)

  override def getObject(columnLabel: String): AnyRef = getColumn[AnyRef](columnLabel)

  override def getMetaData: ResultSetMetaData = metadata

  override def getWarnings: SQLWarning = None.orNull

  override def wasNull: Boolean = lastColumnNull

  private def getColumn[V <: AnyRef](columnLabel: String)(implicit ev: ClassTag[V]): V = {
    getColumn[V](getColumnIndex(columnLabel))
  }

  private def getColumn[V <: AnyRef](columnIndex: Int)(implicit ev: ClassTag[V]): V = {
    checkRow(columnIndex)
    val result = inferValue[V](columnIndex)
    lastColumnNull = Option(result).forall(_ => false)
    result
  }

  private def checkRow(columnIndex: Int): Unit = {
    def checkIfEmpty(): Unit = if (isEmpty) throw EmptyRow()

    def checkColumnBounds(index: Int): Unit = {
      val (min, max) = getColumnBounds
      if (index < min || index > max)
        throw InvalidColumn(s"Column with index $index does not exist")
    }

    checkIfEmpty()
    checkColumnBounds(columnIndex)
  }

  private def inferValue[V <: AnyRef](columnIndex: Int)(implicit ev: ClassTag[V]): V = {
    val value = getValue[V](columnIndex)

    import ImplicitClasses._
    ev.runtimeClass match {
      case Any_ if ev.runtimeClass == Option(value).map(_.getClass).getOrElse(classOf[Object]) => value
      case String_ => Option(value).map(_.toString).getOrElse(None.orNull)
      case JBoolean_ if value.isInstanceOf[String] => JBoolean.parseBoolean(value.asInstanceOf[String])
      case JBoolean_ if value.isInstanceOf[Number] => value.asInstanceOf[Number].intValue != 0
      case JShort_ if value.isInstanceOf[String] => JShort.parseShort(value.asInstanceOf[String])
      case JShort_ if value.isInstanceOf[Number] => value.asInstanceOf[Number].shortValue
      case JShort_ if value.isInstanceOf[JBoolean] => value.asInstanceOf[JBoolean].compareTo(false).shortValue
      case JInt_ if value.isInstanceOf[String] => JInt.parseInt(value.asInstanceOf[String])
      case JInt_ if value.isInstanceOf[Number] => value.asInstanceOf[Number].intValue
      case JInt_ if value.isInstanceOf[JBoolean] => value.asInstanceOf[JBoolean].compareTo(false).intValue
      case JLong_ if value.isInstanceOf[String] => JLong.parseLong(value.asInstanceOf[String])
      case JLong_ if value.isInstanceOf[Number] => value.asInstanceOf[Number].longValue
      case JLong_ if value.isInstanceOf[JBoolean] => value.asInstanceOf[JBoolean].compareTo(false).longValue
      case JDouble_ if value.isInstanceOf[String] => JDouble.parseDouble(value.asInstanceOf[String])
      case JDouble_ if value.isInstanceOf[Number] => value.asInstanceOf[Number].doubleValue
      case JDouble_ if value.isInstanceOf[JBoolean] => value.asInstanceOf[JBoolean].compareTo(false).doubleValue
      case JFloat_ if value.isInstanceOf[String] => JFloat.parseFloat(value.asInstanceOf[String])
      case JFloat_ if value.isInstanceOf[Number] => value.asInstanceOf[Number].floatValue
      case JFloat_ if value.isInstanceOf[JBoolean] => value.asInstanceOf[JBoolean].compareTo(false).floatValue
      case JByte_ if value.isInstanceOf[String] => value.asInstanceOf[String].toByte
      case JByte_ if value.isInstanceOf[Number] => value.asInstanceOf[Number].byteValue
      case JByte_ if value.isInstanceOf[JBoolean] => value.asInstanceOf[JBoolean].compareTo(false).byteValue
      case JByteArray_ if value.isInstanceOf[String] => value.asInstanceOf[String].getBytes
      case JByteArray_ if value.isInstanceOf[Number] => scala.Array[Byte](value.asInstanceOf[Number].byteValue)
      case JByteArray_ if value.isInstanceOf[JBoolean] =>
        scala.Array[Byte](value.asInstanceOf[JBoolean].compareTo(false).byteValue)
      case _ => value
    }
    }.asInstanceOf[V]

  private object ImplicitClasses {
    val Any_ : Class[Any] = classOf[Any]
    val String_ : Class[String] = classOf[String]
    val JBoolean_ : Class[JBoolean] = classOf[JBoolean]
    val JShort_ : Class[JShort] = classOf[JShort]
    val JInt_ : Class[JInt] = classOf[JInt]
    val JLong_ : Class[JLong] = classOf[JLong]
    val JDouble_ : Class[JDouble] = classOf[JDouble]
    val JFloat_ : Class[JFloat] = classOf[JFloat]
    val JByte_ : Class[JByte] = classOf[JByte]
    val JByteArray_ : Class[scala.Array[Byte]] = classOf[scala.Array[Byte]]
  }

  protected def isEmpty: Boolean = currentRow.isEmpty

  protected def getColumnIndex(columnLabel: String): Int = {
    indexByLabel.getOrElse(columnLabel.toUpperCase, throw InvalidColumn())
  }

  protected def getColumnBounds: (Int, Int)

  protected def getValue[V <: AnyRef](columnIndex: Int): V

}
