package com.lambdai.thriftdao

import com.mongodb.casbah.Imports._
import com.twitter.scrooge.{ThriftEnum, ThriftStruct, ThriftStructCodec}
import org.apache.thrift.protocol._
import scala.util.matching.Regex

trait MongoThriftDao[T <: ThriftStruct, C <: ThriftStructCodec[T]] {
  protected def serializer: DBObjectBsonThriftSerializer[T]
  protected def coll: MongoCollection
  protected def fields: List[TField]
  protected def codec: C

  private def fieldsToDbo(fields: List[TField], f: TField => Any): DBObject = {
    DBObject(fields.map(field => field.id.toString -> f(field)))
  }
  
  def ensureIndex(indexName: String, unique: Boolean, fields: List[TField]): Unit = {
    coll.ensureIndex(
        fieldsToDbo(fields, _ => 1), coll.name + "-" + indexName + "-index", unique)
  }
      
  private val primaryKey = DBObject(fields.map(f => f.id.toString -> 1))
  
  if (primaryKey.size > 0)
    coll.ensureIndex(primaryKey, coll.name + "-primiary-index", true)
  
  def store(obj: T): Unit = {
    val dbo = serializer.toDBObject(obj)
    if (primaryKey.size > 0) {
      val keyDbo = fieldsToDbo(fields, f => dbo(f.id.toString))
      coll.findAndModify(keyDbo, null, null, false, dbo, true, true)
    } else {
      coll.insert(dbo)
    }
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

  private def toDBObject(condition: Traversable[Pair[List[TField], Any]]): DBObject = {
    DBObject(condition.map(kv =>
      toDBObjectValue(kv._1, kv._2)
    ).toList)
  }

  /*
   * Construct DBObject in terms of field inside multiple layers struct. Please provide fields in
   * the order from outside to inside, strictly enter one layer at a time. Will make it smarter
   * in the future.
   *
   * For example:
   *
   * Node {
   *  ...
   *
   *  2: FileLoc fileLoc {
   *    1: projectId string
   *
   *    ...
   *  }
   *
   *  ...
   * }
   *
   * to access "projectId" field of FileLoc in a Node, call this function as:
   *
   *  toDBObjectValue(List(Node.FileLocField, FileLocation.ProjectIdField) -> pid)
   *
   * the returned DBObject will match all the Nodes with fileLoc's projectId in value of 'pid'
   */
  private def toDBObjectValue(fields: List[TField], v: Any) = {
    val key = fields.map(f => f.id.toString).mkString(".")
    val lastField = fields.last

    key -> ((lastField.`type`, v) match {
      case (TType.BOOL, _: Boolean) | (TType.BYTE, _: Byte) | (TType.DOUBLE, _: Double) => v
      case (TType.STRING, _: String) => v
      case (TType.STRING, _: Regex) => v
      case (TType.I32, _: Int) | (TType.I16, _: Short) | (TType.I64, _: Long) => v
      case (TType.LIST, _: List[_]) => v
      case (TType.MAP, _: Map[_, _]) => v
      case (TType.SET, _: Set[_]) => v
      case (TType.ENUM, e: ThriftEnum) => e.getValue() // type unsafe
      case (TType.STRUCT, s: ThriftStruct) => {
        //        val comp = companion(s).asInstanceOf[ThriftStructCodec[_]]
        //        if (f. comp.metaData.structName)
        // TODO get the field's manifest and do the verify xxxFieldManifest
        unsafeToDBObject(s)
      }
      case (_, _) => throw new RuntimeException("thrift type mismatch: %d vs %s".format(lastField.`type`, v.getClass.getSimpleName))
    })
  }

  private def unsafeToDBObject(t: ThriftStruct): DBObject = {
    val comp: ThriftStructCodec[ThriftStruct] = ReflectionHelper.companion(t)
    val serializer = new DBObjectBsonThriftSerializer[ThriftStruct] {
      val codec = comp
    }
    serializer.toDBObject(t)
  }
}