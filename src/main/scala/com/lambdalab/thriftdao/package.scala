package com.lambdalab

import com.mongodb.DBObject
import org.apache.thrift.protocol.TField

package object thriftdao {
  def $(fields: TField*) = {
    FieldSelector(fields: _*)
  }

  type FieldAssoc =  (FieldSelector, Any)
  type FieldFilter =  (FieldSelector, DBObject)

}
