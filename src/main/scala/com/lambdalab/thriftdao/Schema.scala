package com.lambdalab.thriftdao

import com.mongodb.casbah.Imports._
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}

class Schema[T <: ThriftStruct, C <: ThriftStructCodec[T]](codec: C, primaryKey: List[FieldSelector], indexes: List[Index[C]]) {
  def tracerFactory(col: MongoCollection): DbTracer = DefaultDbTracer
  /*
   * take a mongodb object and create the actual data access object
   */
  def connect(db: MongoDB): MongoThriftDao[T, C] = {
    val collection = db(codec.metaData.structName)
    val dao = new MongoThriftDao[T, C] {
      def codec = Schema.this.codec
      def serializer = DBObjectBsonThriftSerializer(codec)
      def coll = collection
      def primaryFields = primaryKey
      def tracer = tracerFactory(collection)
    }
    
    for (index @ Index(name, unique, fields, nfields) <- indexes) {
      dao.ensureIndexNested(name, unique, index.toDBObject(codec))
    }
    dao
  }
}

