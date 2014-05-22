package com.lambdalab.thriftdao

import org.apache.thrift.protocol.TField

case class Index[C](name: String, unique: Boolean, fields: List[C => TField], nfields: List[List[TField]] = Nil)
