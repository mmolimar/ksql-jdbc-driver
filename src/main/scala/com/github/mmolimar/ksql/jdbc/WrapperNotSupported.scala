package com.github.mmolimar.ksql.jdbc

import java.sql.Wrapper

import com.github.mmolimar.ksql.jdbc.Exceptions._

trait WrapperNotSupported extends Wrapper {

  override def unwrap[T](iface: Class[T]): T = throw NotSupported("unknown")

  override def isWrapperFor(iface: Class[_]): Boolean = throw NotSupported("unknown")

}
