package com.lambdalab.thriftdao

import com.lambdalab.thriftdao.bson.LTBSONProtocol
import com.mongodb.casbah.Imports._
import com.mongodb.{DefaultDBDecoder, DefaultDBEncoder}
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec, ThriftStructSerializer}
import org.bson.BSONObject

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

  def fromDBObject(obj: BSONObject): T = {
    obj.removeField("_id")
    val bytes = bsonEncoder.encode(obj)
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
    serializerCache.get() += _codec.asInstanceOf[ThriftStructCodec[ThriftStruct]] -> s.asInstanceOf[DBObjectBsonThriftSerializer[ThriftStruct]]
    s
  }

  private val serializerCache = new ThreadLocal[mutable.Map[ThriftStructCodec[ThriftStruct], DBObjectBsonThriftSerializer[ThriftStruct]]] {
    override def initialValue() = {
      mutable.Map[ThriftStructCodec[ThriftStruct], DBObjectBsonThriftSerializer[ThriftStruct]]()
    }
  }

  def unsafeToDBObject(t: ThriftStruct): DBObject = {
    val comp: ThriftStructCodec[ThriftStruct] = ReflectionHelper.companion(t)
    val serializer = serializerCache.get().getOrElseUpdate(comp, new DBObjectBsonThriftSerializer[ThriftStruct] {
      val codec = comp
    })
    serializer.toDBObject(t)
  }
}
