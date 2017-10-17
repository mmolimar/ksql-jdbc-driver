package com.github.mmolimar.ksql.jdbc

import java.sql.SQLFeatureNotSupportedException

import org.scalatest.{Matchers, WordSpec}

import scala.reflect.runtime.universe._


class KsqlDatabaseMetaDataSpec extends WordSpec with Matchers {

  val implementedMethods = Seq("getDriverName", "getDriverVersion", "getDriverMajorVersion", "getDriverMinorVersion",
    "getJDBCMajorVersion", "getJDBCMinorVersion")

  "A KsqlDatabaseMetaData" when {

    val metadata = new KsqlDatabaseMetaData

    "validating specs" should {

      val declaredMembers = typeOf[KsqlDatabaseMetaData].decls

      "throw not supported exception if not supported" in {
        reflectMethods(declaredMembers, false, metadata)
          .foreach(method => {
            assertThrows[SQLFeatureNotSupportedException] {
              try {
                method()
              } catch {
                case t: Throwable => throw t.getCause
              }
            }
          })
      }
      "work if implemented" in {
        reflectMethods(declaredMembers, true, metadata)
          .foreach(method => {
            method()
          })
      }
    }
  }


  private def reflectMethods(decls: MemberScope, implemented: Boolean,
                             metadata: KsqlDatabaseMetaData): Seq[() => Any] = {
    decls
      .filter(_.overrides.size > 0)
      .filter(ms => implementedMethods.contains(ms.name.toString) == implemented)
      .map(_.asMethod)
      .map(m => {

        val args = new Array[AnyRef](m.paramLists(0).size)
        for ((paramType, index) <- m.paramLists(0).zipWithIndex) {
          args(index) = paramType.info match {
            case tof if tof == typeOf[Int] => Int.box(0)
            case tof if tof == typeOf[Boolean] => Boolean.box(false)
            case _ => null
          }
        }

        val mirror = runtimeMirror(classOf[KsqlDatabaseMetaData].getClassLoader).reflect(metadata)
        val method = mirror.reflectMethod(m)
        () => method(args: _*)
      }).toSeq
  }

}
