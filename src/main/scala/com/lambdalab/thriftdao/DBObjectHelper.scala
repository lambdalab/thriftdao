package com.lambdalab.thriftdao

import com.mongodb.casbah.Imports._
import com.twitter.scrooge.{ThriftEnum, ThriftStruct}
import org.apache.thrift.protocol.{TField, TType}

import scala.util.matching.Regex

trait DBObjectHelper {
  protected def toDBObject(condition: Traversable[(List[TField], Any)]): DBObject = {
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
  protected def toDBObjectValue(fields: List[TField], v: Any) = {
    val key = toLabel(fields)
    val lastField = fields.last

    key -> ((lastField.`type`, v) match {
      case (TType.BOOL, _: Boolean) | (TType.BYTE, _: Byte) | (TType.DOUBLE, _: Double) => v
      case (TType.STRING, _: String) => v
      case (TType.STRING, _: Regex) => v
      case (TType.I32, _: Int) | (TType.I16, _: Short) | (TType.I64, _: Long) => v
      case (TType.LIST, l: Traversable[_]) => {
        if (l.isEmpty) l else {
          l.head match {
            case l1: ThriftStruct => l.map(e => DBObjectBsonThriftSerializer.unsafeToDBObject(e.asInstanceOf[ThriftStruct]))
            case _ => l
          }
        }
      }
      case (TType.MAP, _: Map[_, _]) => v
      case (TType.SET, _: Set[_]) => v
      case (TType.ENUM, e: ThriftEnum) => e.getValue() // type unsafe
      case (TType.STRUCT, s: ThriftStruct) => {
        //        val comp = companion(s).asInstanceOf[ThriftStructCodec[_]]
        //        if (f. comp.metaData.structName)
        // TODO get the field's manifest and do the verify xxxFieldManifest

        DBObjectBsonThriftSerializer.unsafeToDBObject(s)
      }
      case (_, _) => throw new RuntimeException("thrift type mismatch: %d vs %s".format(lastField.`type`, v.getClass.getSimpleName))
    })
  }

  protected def toLabel(fields: List[TField]) = {
    fields.map(f => f.id.toString).mkString(".")
  }
}
