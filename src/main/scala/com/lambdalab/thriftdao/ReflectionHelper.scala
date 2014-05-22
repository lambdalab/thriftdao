package com.lambdalab.thriftdao

//import scala.reflect.runtime.currentMirror

object ReflectionHelper {
//  def companion[T](obj: Any): T = {
//    val classSymbol = currentMirror.classSymbol(obj.getClass)
//    val moduleSymbol = classSymbol.companionSymbol.asModule
//    val moduleMirror = currentMirror.reflectModule(moduleSymbol)
//    moduleMirror.instance.asInstanceOf[T]
//  }

  def companion[T](obj: Any): T = {
    val structClass = obj.getClass
    val companion = Class.forName(structClass.getName + "$")
    companion.getField("MODULE$").get(null).asInstanceOf[T]
  }

}
