package com.lambdai.thriftdao

import com.mongodb.casbah.Imports._
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import org.apache.thrift.protocol._

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
    
    for (Index(name, unique, fields, nfields) <- indexes) {
      dao.ensureIndexNested(name, unique, fields.map(f => List(f(codec))) ++ nfields)
    }
    dao
  }
}

