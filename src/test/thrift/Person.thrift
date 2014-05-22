namespace java com.lambdalab.thriftdao

enum Nationality {
  CHINA
  USA
}

struct Name {
  1: string firstName
  2: string lastName
  3: optional string nickName
}

struct Person {
  1: Name name
  2: optional Nationality nationality
}
