package com.lambdai.thriftdao

import com.twitter.scrooge.{ThriftStruct, ThriftStructSerializer, ThriftStructCodec}
import com.foursquare.common.thrift.bson.TBSONProtocol
import com.mongodb.casbah.Imports._
import com.mongodb.{DefaultDBDecoder, DefaultDBEncoder}

/**
 * This class assume that the first field is the index
 */
trait DBObjectBsonThriftSerializer[T <: ThriftStruct] {
  def codec: ThriftStructCodec[T]

  val bsonDecoder = new DefaultDBDecoder
  val bsonEncoder = new DefaultDBEncoder

  val serializer = new ThriftStructSerializer[T] {
    val protocolFactory = new TBSONProtocol.WriterFactory
    def codec = DBObjectBsonThriftSerializer.this.codec
  }

  val deserializer = new ThriftStructSerializer[T] {
    val protocolFactory = new TBSONProtocol.ReaderFactory
    def codec = DBObjectBsonThriftSerializer.this.codec
  }

  def fromDBObject(obj: DBObject): T = {
    val bytes = bsonEncoder.encode(obj - "_id")
    deserializer.fromBytes(bytes)
  }

  def toDBObject(obj: T): DBObject = {
    bsonDecoder.decode(serializer.toBytes(obj), null)
  }
}

object DBObjectBsonThriftSerializer {
  def apply[T <: ThriftStruct](_codec: ThriftStructCodec[T]): DBObjectBsonThriftSerializer[T] = {
    new DBObjectBsonThriftSerializer[T] {
      def codec = _codec
    }
  }
}
