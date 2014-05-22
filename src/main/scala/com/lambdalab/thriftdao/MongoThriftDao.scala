package com.lambdalab.thriftdao

import com.mongodb.casbah.Imports._

import com.twitter.scrooge.{ThriftEnum, ThriftStruct, ThriftStructCodec}
import org.apache.thrift.protocol._
import scala.util.matching.Regex

trait MongoThriftDao[T <: ThriftStruct, C <: ThriftStructCodec[T]] extends DBObjectHelper {
  protected def serializer: DBObjectBsonThriftSerializer[T]
  protected def coll: MongoCollection
  protected def primaryFields: List[TField]
  protected def codec: C

  def ensureIndex(indexName: String, unique: Boolean, fields: List[TField]): Unit = {
    coll.ensureIndex(
      DBObject(fields.map(field => field.id.toString -> 1)),
      coll.name + "-" + indexName + "-index", unique)
  }

  def ensureIndexNested(indexName: String, unique: Boolean, nfields: List[List[TField]]): Unit = {
    coll.ensureIndex(
      DBObject(nfields.map(fields => toLabel(fields) -> 1)),
      coll.name + "-" + indexName + "-index", unique)
  }

  private val primaryKey = DBObject(primaryFields.map(f => f.id.toString -> 1))

  if (primaryKey.size > 1) { // If it == 1, we'll map it to _id and it get indexed automatically
    coll.ensureIndex(primaryKey, coll.name + "-primiary-index", true)
  }

  private def withId(dbo: DBObject): DBObject = {
    if (primaryKey.size == 1) {
      val k = primaryFields(0).id.toString
      if (dbo.contains(k)) {
        val v = dbo(k)
        dbo -= k
        dbo += "_id" -> v
      }
    }
    dbo
  }

  private def toDBObjectWithId(t: T) = {
    val dbo = serializer.toDBObject(t)
    withId(dbo)
  }

  private def fromDBObjectWithId(dbo: DBObject): T = {
    if (primaryKey.size == 1) {
      val k = primaryFields(0).id.toString
      val v = dbo("_id")
      dbo -= "_id"
      dbo += k -> v
    }
    serializer.fromDBObject(dbo)
  }

  def store(obj: T): Unit = {
    val dbo = toDBObjectWithId(obj)
    if (primaryKey.size > 0) {

      val keyDbo = {
        if (primaryKey.size == 1) DBObject("_id" -> dbo("_id"))
        else filter(dbo, primaryFields)
      }
      coll.findAndModify(keyDbo, null, null, false, dbo, true, true)
    } else {
      coll.insert(dbo)
    }
  }

  def insert(objs: List[T]): Unit = {
    val dbos = objs.map(toDBObjectWithId)
    coll.insert(dbos: _*)(x => x, concern = WriteConcern.Normal.continueOnError(true)) // TODO, investigate why implicit doesn't work
  }

  def findAll(): Iterator[T] = {
    coll.find().map(dbo => fromDBObjectWithId(dbo))
  }

  def removeOne(condition: Pair[TField, Any]*): Option[T] = {
    removeOneNested(condition.map(c => List(c._1) -> c._2): _*)
  }

  def removeOneNested(condition: Pair[List[TField], Any]*): Option[T] = {
    coll.findAndRemove(withId(toDBObject(condition))).map(dbo => fromDBObjectWithId(dbo))
  }

  def removeAll(condition: Pair[TField, Any]*) = {
    removeAllNested(condition.map(c => List(c._1) -> c._2): _*)
  }

  def removeAllNested(condition: Pair[List[TField], Any]*) = {
    coll.remove(withId(toDBObject(condition)))
  }

  def find(condition: Pair[TField, Any]*): Iterator[T] = {
    findNested(condition.map(c => List(c._1) -> c._2): _*)
  }

  def findNested(condition: Pair[List[TField], Any]*): Iterator[T] = {
    coll.find(withId(toDBObject(condition))).map(dbo => fromDBObjectWithId(dbo))
  }

  def findOne(condition: Pair[TField, Any]*): Option[T] = {
    findOneNested(condition.map(c => List(c._1) -> c._2): _*)
  }

  def findOneNested(condition: Pair[List[TField], Any]*): Option[T] = {
    coll.findOne(withId(toDBObject(condition))).map(dbo => fromDBObjectWithId(dbo))
  }

  def update(condition: Traversable[Pair[TField, Any]], set: Traversable[Pair[TField, Any]], inc: Traversable[Pair[TField, AnyVal]] = Nil): Unit = {
    updateNested(condition.map(c => List(c._1) -> c._2), set.map(s => List(s._1) -> s._2), inc.map(i => List(i._1) -> i._2))
  }

  def updateNested(condition: Traversable[Pair[List[TField], Any]], set: Traversable[Pair[List[TField], Any]], inc: Traversable[Pair[List[TField], AnyVal]] = Nil): Unit = {
    val query = withId(toDBObject(condition))
    val setObj = toDBObject(set)
    val incObj = toDBObject(inc)
    val updateObj = {
      if (incObj.isEmpty) DBObject("$set" -> setObj)
      else DBObject("$set" -> setObj, "$inc" -> incObj)
    }
    coll.update(query, updateObj, upsert = false, multi = true)
  }
}