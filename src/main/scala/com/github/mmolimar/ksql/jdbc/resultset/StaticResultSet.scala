package com.github.mmolimar.ksql.jdbc.resultset

import java.sql.ResultSetMetaData

import com.github.mmolimar.ksql.jdbc.HeaderField


class StaticResultSet[T <: Any](private val metadata: ResultSetMetaData,
                                private[jdbc] val rows: Iterator[Seq[T]]) extends AbstractResultSet(metadata, rows) {

  def this(columns: List[HeaderField], rows: Iterator[Seq[T]]) =
    this(new KsqlResultSetMetadata(columns), rows)

  override protected def getValue[V <: AnyRef](columnIndex: Int): V = currentRow.get(columnIndex - 1).asInstanceOf[V]

  override protected def getColumnBounds: (Int, Int) = (1, currentRow.getOrElse(Seq.empty).size)

  override protected def closeInherit: Unit = {}

}
