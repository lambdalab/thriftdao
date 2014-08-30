package com.lambdalab

import org.apache.thrift.protocol.TField

package object thriftdao {
  def $(fields: TField*) = {
    FieldSelector(fields: _*)
  }

  type FieldAssoc =  Pair[FieldSelector,Any]
}
