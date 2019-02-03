package com.github.mmolimar.ksql.jdbc.resultset

import java.io.Closeable
import java.sql.{ResultSet, ResultSetMetaData}
import java.util.Iterator

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc.InvalidColumn
import io.confluent.ksql.GenericRow
import io.confluent.ksql.rest.client.KsqlRestClient
import io.confluent.ksql.rest.entity.StreamedRow

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Success, Try}

private[jdbc] class KsqlQueryStream(stream: KsqlRestClient.QueryStream)
  extends Closeable with Iterator[StreamedRow] {

  override def close: Unit = stream.close

  override def hasNext: Boolean = stream.hasNext

  override def next: StreamedRow = stream.next

}

class KsqlResultSet(private val metadata: ResultSetMetaData,
                    private val stream: KsqlQueryStream, val timeout: Long = 0)
  extends AbstractResultSet[StreamedRow](metadata, stream) {

  private val emptyRow: StreamedRow = StreamedRow.row(new GenericRow)

  private val waitDuration = if (timeout > 0) timeout millis else Duration.Inf

  protected override def nextResult: Boolean = {
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

  override protected def closeInherit: Unit = stream.close

  override protected def getColumnBounds: (Int, Int) = (1, currentRow.getOrElse(emptyRow).getRow.getColumns.size)

  override protected def getValue[T](columnIndex: Int): T = {
    currentRow.map(_.getRow.getColumns.get(columnIndex - 1)).getOrElse(throw InvalidColumn()).asInstanceOf[T]
  }

  override def getConcurrency: Int = ResultSet.CONCUR_READ_ONLY

  override def isAfterLast: Boolean = false

  override def isBeforeFirst: Boolean = false

  override def isFirst: Boolean = currentRow.isEmpty

  override def isLast: Boolean = !stream.hasNext

}
