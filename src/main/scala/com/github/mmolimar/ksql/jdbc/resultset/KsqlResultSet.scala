package com.github.mmolimar.ksql.jdbc.resultset

import java.io.Closeable
import java.sql.ResultSet
import java.util.Iterator

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc.NotSupported
import io.confluent.ksql.GenericRow
import io.confluent.ksql.rest.client.KsqlRestClient
import io.confluent.ksql.rest.entity.StreamedRow

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Success, Try}

private[resultset] class JdbcQueryStream(stream: KsqlRestClient.QueryStream, timeout: Long)
  extends Closeable with Iterator[StreamedRow] {

  override def close(): Unit = stream.close

  override def hasNext: Boolean = stream.hasNext

  override def next(): StreamedRow = stream.next

}

class KsqlResultSet(private[jdbc] val stream: JdbcQueryStream, val timeout: Long = 0)
  extends AbstractResultSet[StreamedRow](stream) {

  def this(stream: KsqlRestClient.QueryStream, timeout: Long) = this(new JdbcQueryStream(stream, timeout))

  private val emptyRow: StreamedRow = StreamedRow.row(new GenericRow)

  private val waitDuration = if (timeout > 0) timeout millis else Duration.Inf

  override def next: Boolean = {
    def hasNext = stream.hasNext match {
      case true =>
        stream.next match {
          case record if Option(record.getRow) == None => false
          case record =>
            currentRow = Some(record)
            true
        }
      case false => false
    }

    Try(Await.result(Future(hasNext), waitDuration)) match {
      case Success(r) => r
      case Failure(_: TimeoutException) => false
      case Failure(e) => throw e
    }
  }

  override def isBeforeFirst: Boolean = false

  override def isAfterLast: Boolean = false

  override def isLast: Boolean = false

  override def isFirst: Boolean = currentRow.isEmpty

  override def getConcurrency: Int = ResultSet.CONCUR_READ_ONLY

  override def close: Unit = stream.close

  override protected def getColumnBounds: (Int, Int) = (1, currentRow.getOrElse(emptyRow).getRow.getColumns.size)

  override protected def getValue[T <: AnyRef](columnIndex: Int): T = {
    currentRow.get.getRow.getColumns.get(columnIndex - 1).asInstanceOf[T]
  }

  override protected def getColumnIndex(columnLabel: String): Int = throw NotSupported("getting column by label")

}
