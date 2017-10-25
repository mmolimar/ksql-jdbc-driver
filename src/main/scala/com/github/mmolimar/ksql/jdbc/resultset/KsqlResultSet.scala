package com.github.mmolimar.ksql.jdbc.resultset

import java.sql.ResultSet

import io.confluent.ksql.GenericRow
import io.confluent.ksql.rest.client.KsqlRestClient
import io.confluent.ksql.rest.entity.StreamedRow

import scala.collection.JavaConversions._


class KsqlResultSet(private[jdbc] val stream: KsqlRestClient.QueryStream) extends AbstractResultSet[StreamedRow](stream) {

  private val emptyRow: StreamedRow = new StreamedRow(new GenericRow, null)

  override def isBeforeFirst: Boolean = false

  override def isAfterLast: Boolean = false

  override def isLast: Boolean = false

  override def isFirst: Boolean = currentRow.isEmpty

  override def getConcurrency: Int = ResultSet.CONCUR_READ_ONLY

  override def close: Unit = stream.close

  override protected def getColumnBounds: (Int, Int) = (0, currentRow.getOrElse(emptyRow).getRow.getColumns.size)

  override protected def getValue[T <: AnyRef](columnIndex: Int): T = {
    currentRow.get.getRow.getColumns.get(columnIndex - 1).asInstanceOf[T]
  }
}
