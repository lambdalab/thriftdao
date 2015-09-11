package com.lambdalab.thriftdao

import com.mongodb.casbah.MongoCollection
import com.twitter.scrooge.{ThriftStructCodec, ThriftStruct}
import org.apache.thrift.protocol.TField
import scala.collection.mutable

trait Schemas {
  def tracerFactory(col: MongoCollection): DbTracer = DefaultTracer
  // TODO merge this with new one
  def create[T <: ThriftStruct](
      codec: ThriftStructCodec[T]) (primaryKey: List[codec.type => TField] = Nil,
      indexes: List[Index[codec.type]] = Nil): Schema[T, codec.type] = {
    new Schema[T, codec.type](codec, primaryKey.map(f => FieldSelector(f(codec))), indexes) {
      override def tracerFactory(col: MongoCollection) = Schemas.this.tracerFactory(col)
    }
  }

  def create2[T <: ThriftStruct](codec: ThriftStructCodec[T]) (primaryKey: List[FieldSelector],
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

  protected val allSchema = new mutable.HashMap[ThriftStructCodec[_], AnyRef]
}
//
//object Schemas extends Schemas {
//
//}