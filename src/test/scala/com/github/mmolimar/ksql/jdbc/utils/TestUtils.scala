package com.github.mmolimar.ksql.jdbc.utils

import scala.reflect.{ClassTag, _}
import scala.reflect.runtime.universe._

object TestUtils {

  def reflectMethods[T <: AnyRef](implementedMethods: Seq[String], implemented: Boolean,
                                  obj: T)(implicit tt: TypeTag[T], ct: ClassTag[T]): Seq[() => Any] = {

    typeTag.tpe.decls
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

        val mirror = runtimeMirror(classTag[T].runtimeClass.getClassLoader).reflect(obj)
        val method = mirror.reflectMethod(m)
        () => method(args: _*)
      }).toSeq
  }

}
