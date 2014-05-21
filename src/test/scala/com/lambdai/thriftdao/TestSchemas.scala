package com.lambdai.thriftdao

object TestSchemas extends Schemas {
  apply(
    Person -> create(Person) (
      primaryKey = List(_.NameField),
      indexes = List(
        Index("Nation", false, List(_.NationalityField))
      )
    )
  )
  apply(
    SimpleStruct -> create(SimpleStruct) (
      primaryKey = Nil
    )
  )
}