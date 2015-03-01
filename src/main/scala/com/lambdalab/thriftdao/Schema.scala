package com.lambdalab.thriftdao

import com.mongodb.casbah.Imports._
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import org.apache.thrift.protocol._

class Schema[T <: ThriftStruct, C <: ThriftStructCodec[T]](codec: C, primaryKey: List[FieldSelector], indexes: List[Index[C]]) {
  /*
   * take a mongodb object and create the actual data access object
   */
  def connect(db: MongoDB): MongoThriftDao[T, C] = {
    val dao = new MongoThriftDao[T, C] {
      def codec = Schema.this.codec
      def serializer = DBObjectBsonThriftSerializer(codec)
      def coll = db(codec.metaData.structName)
      def primaryFields = primaryKey
    }
    
    for (index @ Index(name, unique, fields, nfields) <- indexes) {
      dao.ensureIndexNested(name, unique, index.toDBObject(codec))
    }
    dao
  }
}

