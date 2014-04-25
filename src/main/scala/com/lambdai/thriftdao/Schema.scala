package com.lambdai.thriftdao

import com.mongodb.casbah.Imports._
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import org.apache.thrift.protocol._

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
      def primaryFields = primaryKey.map(_(codec))
    }
    
    for (Index(name, unique, fields) <- indexes) {
      dao.ensureIndex(name, unique, fields.map(_(codec)))
    }
    dao
  }
}

