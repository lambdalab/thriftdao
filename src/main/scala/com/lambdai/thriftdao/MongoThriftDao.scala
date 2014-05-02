package com.lambdai.thriftdao

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
      
  private val primaryKey = DBObject(primaryFields.map(f => f.id.toString -> 1))
  
  if (primaryKey.size > 0)
    coll.ensureIndex(primaryKey, coll.name + "-primiary-index", true)
  
  def store(obj: T): Unit = {
    val dbo = serializer.toDBObject(obj)
    if (primaryKey.size > 0) {
      val keyDbo = filter(dbo, primaryFields)
      coll.findAndModify(keyDbo, null, null, false, dbo, true, true)
    } else {
      coll.insert(dbo)
    }
  }

  def insert(objs: List[T]): Unit = {
    val dbos = objs.map(serializer.toDBObject(_))
    coll.insert(dbos: _*)(x => x, concern = WriteConcern.Normal.continueOnError(true)) // TODO, investigate why implicit doesn't work
  }

  def findAll(): Iterator[T] = {
    coll.find().map(dbo => serializer.fromDBObject(dbo))
  }

  def removeOne(condition: Pair[TField, Any]*): Option[T] = {
    removeOneNested(condition.map(c => List(c._1) -> c._2): _*)
  }

  def removeOneNested(condition: Pair[List[TField], Any]*): Option[T] = {
    coll.findAndRemove(toDBObject(condition)).map(dbo => serializer.fromDBObject(dbo))
  }

  def removeAll(condition: Pair[TField, Any]*) = {
    removeAllNested(condition.map(c => List(c._1) -> c._2): _*)
  }

  def removeAllNested(condition: Pair[List[TField], Any]*) = {
    coll.remove(toDBObject(condition))
  }

  def find(condition: Pair[TField, Any]*): Iterator[T] = {
    findNested(condition.map(c => List(c._1) -> c._2): _*)
  }

  def findNested(condition: Pair[List[TField], Any]*): Iterator[T] = {
    coll.find(toDBObject(condition)).map(dbo => serializer.fromDBObject(dbo))
  }

  def findOne(condition: Pair[TField, Any]*): Option[T] = {
    findOneNested(condition.map(c => List(c._1) -> c._2): _*)
  }

  def findOneNested(condition: Pair[List[TField], Any]*): Option[T] = {
    coll.findOne(toDBObject(condition)).map(dbo => serializer.fromDBObject(dbo))
  }

  def update(condition: Traversable[Pair[TField, Any]], set: Traversable[Pair[TField, Any]], inc: Traversable[Pair[TField, AnyVal]] = Nil): Unit = {
    updateNested(condition.map(c => List(c._1) -> c._2), set.map(s => List(s._1) -> s._2), inc.map(i => List(i._1) -> i._2))
  }

  def updateNested(condition: Traversable[Pair[List[TField], Any]], set: Traversable[Pair[List[TField], Any]], inc: Traversable[Pair[List[TField], AnyVal]] = Nil): Unit = {
    val query = toDBObject(condition)
    val setObj = toDBObject(set)
    val incObj = toDBObject(inc)
    val updateObj = {
      if (incObj.isEmpty) DBObject("$set" -> setObj)
      else DBObject("$set" -> setObj, "$inc" -> incObj)
    }
    coll.update(query, updateObj, upsert = false, multi = true)
  }
}