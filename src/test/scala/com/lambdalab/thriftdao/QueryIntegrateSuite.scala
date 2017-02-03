package com.lambdalab.thriftdao

import org.scalatest.FunSuite
import com.mongodb.casbah.Imports._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class QueryIntegrateSuite extends FunSuite {
  // TODO Start mongodb server and perform actual query

  val schema = TestSchemas(SimpleStruct)
  var dao: MongoThriftDao[SimpleStruct, SimpleStruct.type] = null

  val cl =  MongoClient("localhost")  // TODO have a better setup of the test
  var db: MongoDB = null
  try {
    cl.dropDatabase("Test")
    db = cl("Test")
    dao = schema.connect(db)
  } catch {
    case e: Throwable => {
      //      e.printStackTrace()
      printf("DB NOT CONNECTED")
    }
  }

  test("store node") {
    if (db != null) {
      val n = SimpleStruct("x", Some("y"), Some(1))
      dao.store(n)
      assert(dao.findOne(SimpleStruct.StrField -> "x") == Some(n))
    }
  }

  test("update field") {
    if (db != null) {
      val n = SimpleStruct("z")
      dao.store(n)
//      dao.update(List(SimpleStruct.StrField -> "x"), List(SimpleStruct.StrField -> "y"))
      dao.select(SimpleStruct.StrField -> "z").set($(SimpleStruct.StrField) -> "y")
      assert(dao.findOne(SimpleStruct.StrField -> "y") == Some(SimpleStruct("y")))
    }
  }

  test("should ingore duplicate key error") {
    if (db != null) {
      val n = SimpleStruct("x", Some("y"), Some(1))
      val n2 = SimpleStruct("x", Some("y"), Some(2))

      dao.removeAll()
      dao.insert(Seq(n))
      dao.insert(Seq(n, n2))
      assert(dao.findAll().size == 2)
    }
  }
}
