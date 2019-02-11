package com.github.mmolimar.ksql

import java.sql.ResultSet

package object jdbc {

  object implicits {

    implicit class ResultSetStream(resultSet: ResultSet) {

      def toStream: Stream[ResultSet] = new Iterator[ResultSet] {

        def hasNext = resultSet.next

        def next = resultSet

      }.toStream
    }

    implicit def toIndexedMap(headers: List[HeaderField]): Map[Int, HeaderField] = {
      headers.zipWithIndex.map { case (header, index) => {
        HeaderField(header.name, header.label, header.jdbcType, header.length, index + 1)
      }
      }.map(h => h.index -> h).toMap
    }
  }

}
