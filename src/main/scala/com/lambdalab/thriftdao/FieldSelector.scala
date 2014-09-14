package com.lambdalab.thriftdao

import com.mongodb.DBObject
import org.apache.thrift.protocol.TField

case class FieldSelector(fields: TField*) {
  def -> (value: Any): FieldAssoc = {
    (this, value)
  }

  def ~-> (value: DBObject): FieldFilter = {
    (this, value)
  }

  def toDBKey = {
    fields.map(_.id.toString).mkString(".")
  }
}