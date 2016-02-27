package com.lambdalab.thriftdao

import com.mongodb.casbah.Imports._
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import org.apache.thrift.protocol._

trait MongoThriftDao[T <: ThriftStruct, C <: ThriftStructCodec[T]] extends DBObjectHelper {
  protected def serializer: DBObjectBsonThriftSerializer[T]
  protected def coll: MongoCollection
  protected def primaryFields: List[FieldSelector]
  protected def codec: C
  protected def tracer: DbTracer

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

  private val primaryKey = DBObject(primaryFields.map(f => f.toDBKey -> 1))

//  if (primaryKey.size > 1) { // If it == 1, we'll map it to _id and it get indexed automatically
//    coll.ensureIndex(primaryKey, coll.name + "-primiary-index", true)
//  }

  private def withId(dbo: DBObject): DBObject = {
    if (primaryKey.size == 1) {
      val k = primaryFields(0).toDBKey
      if (dbo.contains(k)) {
        val v = dbo(k)
        dbo -= k
        dbo += "_id" -> v
      }
    } else if (primaryFields.size > 0) {
      val pValues: Traversable[AnyRef] = primaryFields.map { f =>
        val k = f.toDBKey
        if (!dbo.containsField(k)) None
        else Some(dbo.get(k).toString)
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
      val k = primaryFields(0).toDBKey
      val v = dbo("_id")
      dbo += k -> v
    }
    dbo -= "_id"

    serializer.fromDBObject(dbo)
  }

  def store(obj: T): Unit = {
    tracer.withTracer("store") {
      val dbo = toDBObjectWithId(obj)
      if (primaryKey.size == 1) {
        val keyDbo = DBObject("_id" -> dbo("_id"))
        coll.findAndModify(keyDbo, null, null, false, dbo, true, true)
      } else {
        coll.insert(dbo)
      }
    }
  }

  def insert(objs: Seq[T]): WriteResult = {
    tracer.withTracer("insert") {
      val dbos = objs.map(toDBObjectWithId)
      coll.insert(dbos: _*)(x => x, concern = WriteConcern.Normal.continueOnError(true)) // TODO, investigate why implicit doesn't work
    }
  }

  def findAll(): Iterator[T] = {
    tracer.withTracer("find all") {
      coll.find().map(dbo => fromDBObjectWithId(dbo))
    }
  }

  def removeOne(condition: (TField, Any)*): Option[T] = {
    removeOneNested(condition.map(c => List(c._1) -> c._2): _*)
  }

  def removeOneNested(condition: (List[TField], Any)*): Option[T] = {
    tracer.withTracer("find and remove") {
      coll.findAndRemove(withId(toDBObject(condition))).map(dbo => fromDBObjectWithId(dbo))
    }
  }

  def removeAll(condition: (TField, Any)*) = {
    removeAllNested(condition.map(c => List(c._1) -> c._2): _*)
  }

  def removeAllNested(condition: (List[TField], Any)*) = {
    tracer.withTracer("remove all") {
      coll.remove(withId(toDBObject(condition)))
    }
  }

  def find(condition: (TField, Any)*): Iterator[T] = {
    select(condition.map(convertAssoc) : _*).find()
  }

  def findOne(condition: (TField, Any)*): Option[T] = {
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

    def find(): Iterator[T] = {
      tracer.withTracer("[select] find") {
        coll.find(dbo).map(dbo => fromDBObjectWithId(dbo))
      }
    }

    def find(pageNumber: Int, nPerPage: Int): Iterator[T] = {
      tracer.withTracer("[select] find page %d" format pageNumber) {
        val skipped = if (pageNumber > 0) (pageNumber - 1) * nPerPage else 0
        coll.find(dbo).skip(skipped).limit(nPerPage).map(dbo => fromDBObjectWithId(dbo))
      }
    }

    def find(keys: FieldFilter*): Iterator[T] = {
      tracer.withTracer("[select] find with filter") {
        val filter = DBObject(keys.map(p => p._1.toDBKey -> p._2).toList)
        coll.find(dbo, filter).map(dbo => fromDBObjectWithId(dbo))
      }
    }

    def findOne(): Option[T] = {
      tracer.withTracer("[select] find one") {
        coll.findOne(dbo).map(dbo => fromDBObjectWithId(dbo))
      }
    }

    def exist(): Boolean = {
      tracer.withTracer("[select] exist") {
        coll.find(dbo).limit(1).hasNext
      }
    }

    def remove() = {
      tracer.withTracer("[select] remove") {
        coll.remove(dbo)
      }
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
      tracer.withTracer("[select] update") {
        val query = dbo
        val setObj = withId(toDBObject(getList(set)))
        val incObj = withId(toDBObject(getList(inc)))
        val updateObj = {
          if (incObj.isEmpty) DBObject("$set" -> setObj)
          else DBObject("$set" -> setObj, "$inc" -> incObj)
        }
        coll.update(query, updateObj, upsert = false, multi = true)
      }
    }
  }
}