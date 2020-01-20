package io.confluent.ksql.rest.server

import java.util.function.{Function => JFunction, Supplier => JSupplier}

import io.confluent.ksql.version.metrics.VersionCheckerAgent

import scala.language.implicitConversions

object mock {

  implicit def toJavaSupplier[A](f: () => A): JSupplier[A] = new JSupplier[A] {
    override def get: A = f()
  }

  implicit def toJavaFunction[A, B](f: A => B): JFunction[A, B] = (a: A) => f(a)

  def ksqlRestApplication(config: KsqlRestConfig, versionCheckerAgent: VersionCheckerAgent): KsqlRestApplication = {
    KsqlRestApplication.buildApplication(config, (_: JSupplier[java.lang.Boolean]) => versionCheckerAgent)
  }

}
