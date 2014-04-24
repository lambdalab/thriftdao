package com.lambdai.thriftdao

import com.twitter.scrooge.{ThriftStructCodec, ThriftStruct}
import org.apache.thrift.protocol.TField
import scala.collection.mutable

trait Schemas {
  def create[T <: ThriftStruct](codec: ThriftStructCodec[T]) (primaryKey: List[codec.type => TField],
                                                              indexes: List[Index[codec.type]] = Nil): Schema[T, codec.type] = {
    new Schema[T, codec.type](codec, primaryKey, indexes)
  }

  // Register the schema
  def apply(entry: (ThriftStructCodec[_], AnyRef)) = {
    allSchema += entry
  }

  // Get the schema
  def apply[T <: ThriftStruct](codec: ThriftStructCodec[T]): Schema[T, codec.type] = {
    // this is not very beatiful though
    allSchema(codec).asInstanceOf[Schema[T, codec.type]]
  }

  private val allSchema = new mutable.HashMap[ThriftStructCodec[_], AnyRef]
}
