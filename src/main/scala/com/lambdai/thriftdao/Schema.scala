package com.lambdai.thriftdao

import com.mongodb.casbah.Imports._
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import org.apache.thrift.protocol._
import scala.collection.mutable

case class Index[C](name: String, unique: Boolean, fields: List[C => TField])

class Schema[T <: ThriftStruct, C <: ThriftStructCodec[T]](val codec: C, val primaryKey: List[C => TField], val indexes: List[Index[C]]) {
  /*
   * take a mongodb object and create the actual data access object
   */
  def connect(db: MongoDB): MongoThriftDao[T, C] = {
    val dao = new MongoThriftDao[T, C] {
      def codec = Schema.this.codec
      def serializer = DBObjectBsonThriftSerializer(codec)
      def coll = db(codec.metaData.structName)
      def fields = primaryKey.map(_(codec))
    }
    
    for (Index(name, unique, fields) <- indexes) {
      dao.ensureIndex(name, unique, fields.map(_(codec)))
    }
    dao
  }
}

object Schema {
  def create[T <: ThriftStruct](
      codec: ThriftStructCodec[T]) (primaryKey: List[codec.type => TField],
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
