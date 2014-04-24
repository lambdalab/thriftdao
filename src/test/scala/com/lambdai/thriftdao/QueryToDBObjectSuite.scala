package com.lambdai.thriftdao

import org.scalatest.FunSuite

object TestSchemas extends Schemas {
  apply(
    Person -> create(Person) (
      primaryKey = List(_.NameField),
      indexes = List(
        Index("Nation", false, List(_.NationalityField))
      )
    )
  )
}

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class QueryToDBObjectSuite extends FunSuite {
  // TODO test if the DAO generate the right DBObject for query
}
