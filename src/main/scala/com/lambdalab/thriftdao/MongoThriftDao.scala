package com.lambdalab.thriftdao

import com.mongodb.casbah.Imports._
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import org.apache.thrift.protocol._

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

//  if (primaryKey.size > 1) { // If it == 1, we'll map it to _id and it get indexed automatically
//    coll.ensureIndex(primaryKey, coll.name + "-primiary-index", true)
//  }

  private def withId(dbo: DBObject): DBObject = {
    if (primaryKey.size == 1) {
      val k = primaryFields(0).id.toString
      if (dbo.contains(k)) {
        val v = dbo(k)
        dbo -= k
        dbo += "_id" -> v
      }
    } else if (primaryFields.size > 0) {
      val pValues: Traversable[AnyRef] = primaryFields.map { f =>
        val k = f.id.toString
        dbo.getAs[String](k)
      }.flatten

      if (pValues.size == primaryFields.size) {
        val str = pValues.map(_.toString).mkString
        val key = java.security.MessageDigest.getInstance("MD5").digest(str.getBytes("UTF-8")).take(12)
        dbo += "_id" -> new ObjectId(key)
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
      dbo += k -> v
    }
    dbo -= "_id"

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

  def insert(objs: Seq[T]): WriteResult = {
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
    select(condition.map(convertAssoc) : _*).find()
  }

  def findOne(condition: Pair[TField, Any]*): Option[T] = {
    select(condition.map(convertAssoc) : _*).findOne()
  }

  //////////////////////// New API

  private def convertAssoc(assoc: (TField, Any)): FieldAssoc = {
    FieldSelector(assoc._1) -> assoc._2
  }

  def select(assoc: (TField, Any)) = {
    Select(convertAssoc(assoc))
  }

  def select(assocs: FieldAssoc*) = {
    Select(assocs: _*)
  }

  def update(assocs: Traversable[FieldAssoc], set: Traversable[FieldAssoc], inc: Traversable[FieldAssoc]) = {
    select(assocs.toSeq: _*).update(set, inc)
  }

  case class Select(assocs: FieldAssoc*) {
    private def getList(assocs: Traversable[FieldAssoc]) = {
      assocs.map(p => (p._1.fields.toList, p._2))
    }

    private lazy val dbo = {
      withId(toDBObject(getList(assocs)))
    }

    def find() = {
      coll.find(dbo).map(dbo => fromDBObjectWithId(dbo))
    }

    def find(pageNumber: Int, nPerPage: Int) = {
      val skipped = if (pageNumber > 0) (pageNumber - 1) * nPerPage  else 0
      coll.find(dbo).skip(skipped).limit(nPerPage).map(dbo => fromDBObjectWithId(dbo))
    }

    def find(keys: FieldFilter*) = {
      val filter = DBObject(keys.map(p => p._1.toDBKey -> p._2).toList)
      coll.find(dbo, filter).map(dbo => fromDBObjectWithId(dbo))
    }

    def findOne() = {
      coll.findOne(dbo).map(dbo => fromDBObjectWithId(dbo))
    }

    def exist(): Boolean = {
      coll.find(dbo).limit(1).hasNext
    }

    def remove() = {
      coll.remove(dbo)
    }

    def set(assoc: (TField, Any)): Unit = set(convertAssoc(assoc))
    def set(assocs: FieldAssoc*): Unit = {
      update(set = assocs, inc = Nil)
    }

    def inc(assoc: (TField, Any)): Unit = inc(convertAssoc(assoc))
    def inc(assocs: FieldAssoc*): Unit = {
      update(set = Nil, inc = assocs)
    }

    def update(set: Traversable[FieldAssoc], inc: Traversable[FieldAssoc]) = {
      val query = dbo
      val setObj = toDBObject(getList(set))
      val incObj = toDBObject(getList(inc))
      val updateObj = {
        if (incObj.isEmpty) DBObject("$set" -> setObj)
        else DBObject("$set" -> setObj, "$inc" -> incObj)
      }
      coll.update(query, updateObj, upsert = false, multi = true)
    }
  }
}