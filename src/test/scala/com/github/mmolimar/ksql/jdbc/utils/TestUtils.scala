package com.github.mmolimar.ksql.jdbc.utils

import scala.reflect.runtime.universe._
import scala.reflect.{ClassTag, _}

object TestUtils {

  def reflectMethods[T <: AnyRef](implementedMethods: Seq[String], implemented: Boolean,
                                  obj: T)(implicit tt: TypeTag[T], ct: ClassTag[T]): Seq[() => Any] = {

    val ksqlPackage = "com.github.mmolimar.ksql"
    val declarations = for {
      baseClass <- typeTag.tpe.baseClasses
      if (baseClass.fullName.startsWith(ksqlPackage))
    } yield baseClass.typeSignature.decls

    declarations.flatten
      .filter(_.overrides.size > 0)
      .filter(ms => implementedMethods.contains(ms.name.toString) == implemented)
      .map(_.asMethod)
      .filter(!_.isProtected)
      .map(m => {

        val args = new Array[AnyRef](if (m.paramLists.size == 0) 0 else m.paramLists(0).size)
        if (m.paramLists.size > 0)
          for ((paramType, index) <- m.paramLists(0).zipWithIndex) {
            args(index) = paramType.info match {
              case tof if tof == typeOf[Byte] => Byte.box(0)
              case tof if tof == typeOf[Boolean] => Boolean.box(false)
              case tof if tof == typeOf[Short] => Short.box(0)
              case tof if tof == typeOf[Int] => Int.box(0)
              case tof if tof == typeOf[Double] => Double.box(0)
              case tof if tof == typeOf[Long] => Long.box(0)
              case tof if tof == typeOf[Float] => Float.box(0)
              case tof if tof == typeOf[String] => ""
              case _ => null
            }
          }

        val mirror = runtimeMirror(classTag[T].runtimeClass.getClassLoader).reflect(obj)
        val method = mirror.reflectMethod(m)
        () => method(args: _*)
      })
  }

}
