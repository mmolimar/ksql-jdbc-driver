package com.github.mmolimar.ksql.jdbc.resultset

import com.github.mmolimar.ksql.jdbc.Exceptions._
import com.github.mmolimar.ksql.jdbc.{HeaderField, InvalidColumn}


class StaticResultSet[T <: AnyRef](private[jdbc] val columns: Map[String, HeaderField],
                                   private[jdbc] val rows: Iterator[Seq[T]]) extends AbstractResultSet(rows) {

  override protected def getValue[V <: AnyRef](columnIndex: Int): V = currentRow.get(columnIndex - 1).asInstanceOf[V]

  override protected def getColumnBounds: (Int, Int) = (1, currentRow.getOrElse(Seq.empty).size)

  override protected def getColumnIndex(columnLabel: String): Int = {
    columns.get(columnLabel).getOrElse(throw InvalidColumn()).index
  }
}
