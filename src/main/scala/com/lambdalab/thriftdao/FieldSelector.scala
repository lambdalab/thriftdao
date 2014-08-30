package com.lambdalab.thriftdao

import org.apache.thrift.protocol.TField

case class FieldSelector(fields: TField*) {
  def -> (value: Any): FieldAssoc = {
    (this, value)
  }
}