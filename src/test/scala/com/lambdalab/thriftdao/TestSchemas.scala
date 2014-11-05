package com.lambdalab.thriftdao

object TestSchemas extends Schemas {
  apply(
    Person -> create(Person) (
      primaryKey = List(_.NameField, _.NationalityField),
      indexes = List(
        Index("Nation", false, List(_.NationalityField))
      )
    )
  )
  apply(
    SimpleStruct -> create(SimpleStruct) (
      primaryKey = List(_.StrField, _.Str1Field, _.IntField)
    )
  )
}