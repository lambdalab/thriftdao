package com.lambdalab.thriftdao

import com.twitter.scrooge.{ThriftStruct, ThriftStructSerializer, ThriftStructCodec}
import com.foursquare.common.thrift.bson.TBSONProtocol
import com.mongodb.casbah.Imports._
import com.mongodb.{DefaultDBDecoder, DefaultDBEncoder}
import com.lambdalab.thriftdao.bson.LTBSONProtocol
import scala.collection.mutable

/**
 * This class assume that the first field is the index
 */
trait DBObjectBsonThriftSerializer[T <: ThriftStruct] {
  def codec: ThriftStructCodec[T]

  val bsonDecoder = new DefaultDBDecoder
  val bsonEncoder = new DefaultDBEncoder

  val serializer = new ThriftStructSerializer[T] {
    val protocolFactory = new LTBSONProtocol.WriterFactory
    def codec = DBObjectBsonThriftSerializer.this.codec
  }

  val deserializer = new ThriftStructSerializer[T] {
    val protocolFactory = new LTBSONProtocol.ReaderFactory
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
    val s = new DBObjectBsonThriftSerializer[T] {
      def codec = _codec
    }
    serializerCache += _codec.asInstanceOf[ThriftStructCodec[ThriftStruct]] -> s.asInstanceOf[DBObjectBsonThriftSerializer[ThriftStruct]]
    s
  }

  private val serializerCache = mutable.Map[ThriftStructCodec[ThriftStruct], DBObjectBsonThriftSerializer[ThriftStruct]]()

  def unsafeToDBObject(t: ThriftStruct): DBObject = {
    val comp: ThriftStructCodec[ThriftStruct] = ReflectionHelper.companion(t)
    val serializer = serializerCache.getOrElseUpdate(comp, new DBObjectBsonThriftSerializer[ThriftStruct] {
      val codec = comp
    })
    serializer.toDBObject(t)
  }
}
