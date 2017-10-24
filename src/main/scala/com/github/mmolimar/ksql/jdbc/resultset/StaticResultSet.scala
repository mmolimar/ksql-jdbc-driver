package com.github.mmolimar.ksql.jdbc.resultset


class StaticResultSet[T <: AnyRef](private val iterator: Iterator[Seq[T]]) extends AbstractResultSet(iterator) {

  override protected def getValue[V <: AnyRef](columnIndex: Int): V = currentRow.get(columnIndex).asInstanceOf[V]

  override protected def getColumnBounds: (Int, Int) = (0, 0)

}
