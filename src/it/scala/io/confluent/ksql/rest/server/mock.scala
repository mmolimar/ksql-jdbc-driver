package io.confluent.ksql.rest.server

import java.util.function.{Function => JFunction, Supplier => JSupplier}

import io.confluent.ksql.version.metrics.VersionCheckerAgent

object mock {

  implicit def toJavaSupplier[A](f: Function0[A]): JSupplier[A] = new JSupplier[A] {
    override def get: A = f()
  }

  implicit def toJavaFunction[A, B](f: Function1[A, B]): JFunction[A, B] = new JFunction[A, B] {
    override def apply(a: A): B = f(a)
  }

  def ksqlRestApplication(config: KsqlRestConfig, versionCheckerAgent: VersionCheckerAgent): KsqlRestApplication = {
    KsqlRestApplication.buildApplication(config, (_: JSupplier[java.lang.Boolean]) => versionCheckerAgent)
  }

}
