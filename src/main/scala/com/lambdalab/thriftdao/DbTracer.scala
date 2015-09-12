package com.lambdalab.thriftdao

trait DbTracer {
  def record(msg: String): Unit
  def withTracer[R](operation: String)(f: => R): R  = f
}

object DefaultTracer extends DbTracer {
  override def record(msg: String): Unit = {}
}