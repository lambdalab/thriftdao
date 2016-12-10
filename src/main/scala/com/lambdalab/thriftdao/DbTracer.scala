package com.lambdalab.thriftdao

trait DbTracer {
  def record(msg: String): Unit
  def withTracer[R](operation: String)(f: => R): R  = f
  def disable(): Unit = {}
}

object DefaultDbTracer extends DbTracer {
  override def record(msg: String): Unit = {}
}