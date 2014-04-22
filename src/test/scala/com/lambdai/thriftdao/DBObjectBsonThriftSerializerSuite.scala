package com.lambdai.thriftdao

import org.scalatest.FunSuite

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class DBObjectBsonThriftSerializerSuite extends FunSuite {
  val serializer = DBObjectBsonThriftSerializer(SimpleStruct)
  test("be able to serialize then deserialize") {
    val s1 = SimpleStruct("c")
    val dbo = serializer.toDBObject(s1)
    val s2 = serializer.fromDBObject(dbo)
    assert(s2.field == "c")
    assert(s1 == s2)
  }
}
