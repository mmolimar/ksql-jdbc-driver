package com.github.mmolimar.ksql.jdbc.resultset

import java.io.{Closeable, InputStream}
import java.sql.{ResultSet, ResultSetMetaData}
import java.util.{NoSuchElementException, Scanner, Iterator => JIterator}

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc.{EmptyRow, HeaderField}
import io.confluent.ksql.GenericRow
import io.confluent.ksql.rest.client.QueryStream
import io.confluent.ksql.rest.entity.StreamedRow

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


class IteratorResultSet[T <: Any](private val metadata: ResultSetMetaData, private val maxRows: Long,
                                  private[jdbc] val rows: Iterator[Seq[T]])
  extends AbstractResultSet(metadata, maxRows, rows) {

  def this(columns: List[HeaderField], maxRows: Long, rows: Iterator[Seq[T]]) =
    this(new KsqlResultSetMetaData(columns), maxRows, rows)

  override protected def getValue[V <: AnyRef](columnIndex: Int): V = currentRow.get(columnIndex - 1).asInstanceOf[V]

  override protected def getColumnBounds: (Int, Int) = (1, currentRow.getOrElse(Seq.empty).size)

  override protected def closeInherit(): Unit = {}

}

trait KsqlStream extends Closeable with JIterator[StreamedRow]

private[jdbc] class KsqlQueryStream(stream: QueryStream) extends KsqlStream {

  override def close(): Unit = stream.close()

  override def hasNext: Boolean = stream.hasNext

  override def next: StreamedRow = stream.next

}

private[jdbc] class KsqlInputStream(stream: InputStream) extends KsqlStream {
  private var isClosed = false
  private lazy val scanner = new Scanner(stream)

  override def close(): Unit = {
    isClosed = true
    scanner.close()
  }

  override def hasNext: Boolean = {
    if (isClosed) throw new IllegalStateException("Cannot call hasNext() when stream is closed.")
    scanner.hasNextLine
  }

  override def next: StreamedRow = {
    if (!hasNext) throw new NoSuchElementException
    StreamedRow.row(new GenericRow(scanner.nextLine))
  }

}

class StreamedResultSet(private[jdbc] val metadata: ResultSetMetaData,
                        private[jdbc] val stream: KsqlStream, private[resultset] val maxRows: Long, val timeout: Long = 0)
  extends AbstractResultSet[StreamedRow](metadata, maxRows, stream.asScala) {

  private val waitDuration = if (timeout > 0) timeout millis else Duration.Inf

  private var maxBound = 0

  protected override def nextResult: Boolean = {
    def hasNext = if (stream.hasNext) {
      stream.next match {
        case record if record.getHeader.isPresent && !record.getRow.isPresent =>
          maxBound = record.getHeader.get.getSchema.columns.size
          next
        case record if record.getRow.isPresent =>
          maxBound = record.getRow.get.getColumns.size
          currentRow = Some(record)
          true
        case _ => false
      }
    } else {
      false
    }

    Try(Await.result(Future(hasNext), waitDuration)) match {
      case Success(r) => r
      case Failure(_: TimeoutException) => false
      case Failure(e) => throw e
    }
  }

  override protected def closeInherit(): Unit = stream.close()

  override protected def getColumnBounds: (Int, Int) = (1, maxBound)

  override protected def getValue[T](columnIndex: Int): T = {
    currentRow.filter(_.getRow.isPresent).map(_.getRow.get.getColumnValue[T](columnIndex - 1))
      .getOrElse(throw EmptyRow())
  }

  override def getConcurrency: Int = ResultSet.CONCUR_READ_ONLY

  override def isAfterLast: Boolean = false

  override def isBeforeFirst: Boolean = false

  override def isFirst: Boolean = currentRow.isEmpty

  override def isLast: Boolean = !stream.hasNext

}
