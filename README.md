thriftdao
=========
[![Build Status](https://travis-ci.org/lambdalab/thriftdao.svg?branch=master)](https://travis-ci.org/lambdalab/thriftdao)

Use thrift as IDL and dao (Now only support scrooge -> mongodb)

Usage:
assume you defined as thrift file:

```
struct Name {
  1: string firstName
  2: string lastName
  3: optional string nickName
}

struct Person {
  1: Name name
  2: i32 age
}
```

After you use scrooge to generated the file, you could then declare a schema like:

```
object TestSchemas extends Schemas {
  apply(
    Person -> create(Person) (
      primaryKey = List(_.NameField), // Primary key will be unique indexed by default
      indexes = List(
        Index("Age"/*index name*/, false /*non unique*/, List(_.NationalityField) /*fields to be indexed*/)
      )
    )
  )
}
```

Now you could get a DAO object by doing
```
val dao = TestSchemas(Person).connect(db /* mongodb instance */)
```

Now you could do varies data operations, such as:

```
dao.findOne(Person.AgeField -> 14)
dao.findNested(List(Person.nameField, Name.firstNameField) -> "Philip")
```
you could to update and remove as well
