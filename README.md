
# Nested Objects and Spark User Defined Functions

Suppose you were a museum using Spark to calculate household membership costs.  The calculation goes as follows:  Individual cost is based on age, but only the two most expensive individuals have to pay full price and additional household members only have to pay half price.  Furthermore, there is a 10% discount for households in the museum's district.  While obviously it would be incredibly strange for a museum to use Spark in this way, I chose this example because it emphasizes a need to consider the household as a whole object.  We can't find individual costs and then sum because knowing which two prices are the most expensive requires knowledge about the whole group.

## Grouping Data in Case Classes

If you want to follow along, download the data.csv file.

Often, we receive data that is flat, such as in csv format, even if that means there is lots of redundant information.  If you load the sample data into the spark shell, you'll see this:
```scala
scala> val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("data.csv")
df: org.apache.spark.sql.DataFrame = [firstName: string, lastName: string ... 4 more fields]

scala> df.show
```
```
+---------+--------+---+-------+-----------+-------+
|firstName|lastName|age| street|houseNumber|   city|
+---------+--------+---+-------+-----------+-------+
|     Anne|  Morrow| 33| second|         28| Denver|
|      Zak|  Morrow| 33| second|         28| Denver|
|   Kaylee|  Morrow|  3| second|         28| Denver|
| Jennifer|  Little| 76|juniper|       1986|Boulder|
| Lawrence|  Little| 68|juniper|       1986|Boulder|
|      Amy|  Little| 30|juniper|       1986|Boulder|
|     Tina|  Little|  6|juniper|       1986|Boulder|
|     Lila|  Little|  1|juniper|       1986|Boulder|
|     Todd|   James| 27|  third|         18| Denver|
+---------+--------+---+-------+-----------+-------+
```

User defined functions can send columns to case classes.  Here are the case classes that we'll we using:

```scala
case class Person(firstName: String, lastName: String, age: Int)

case class Address(street: String, houseNumber: Int, city: String)

case class Household(members: Seq[Person], address: Address)
```

Let's start by getting the data organized into People and Addresses.

```scala
import org.apache.spark.sql.functions._

val makePerson = udf(
  (first: String, last: String, age: Int) => Person(first, last, age)
)

val makeAddress = udf(
  (street: String, number: Int, city: String) => Address(street, number, city)
)

val personAddressDF = df.select(
  makePerson(col("firstName"), col("lastName"), col("age")) as "Person",
  makeAddress(col("street"), col("houseNumber"), col("city")) as "Address"
)
```
```
+--------------------+--------------------+
|              Person|             Address|
+--------------------+--------------------+
|  [Anne, Morrow, 33]|[second, 28, Denver]|
|   [Zak, Morrow, 33]|[second, 28, Denver]|
| [Kaylee, Morrow, 3]|[second, 28, Denver]|
|[Jennifer, Little...|[juniper, 1986, B...|
|[Lawrence, Little...|[juniper, 1986, B...|
|   [Amy, Little, 30]|[juniper, 1986, B...|
|   [Tina, Little, 6]|[juniper, 1986, B...|
|   [Lila, Little, 1]|[juniper, 1986, B...|
|   [Todd, James, 27]| [third, 18, Denver]|
+--------------------+--------------------+
```

## Collect Lists

In preparation for creating households, we can group by the address and collect the people into lists of people.  The great news here is that case class equivalence compares all of the constructor values, so doing
```scala
personAddressDF.groupBy("Address")
```
forms the same groups as we would have gotten from
```scala
df.groupBy("street", "houseNumber", "city")
```
if we hadn't clumped these values together as Addresses.
```scala
val almostHouseholdDF = personAddressDF.groupBy("Address")
  .agg(collect_list(col("Person")) as "People")
```
```
+--------------------+--------------------+
|             Address|              People|
+--------------------+--------------------+
|[second, 28, Denver]|[[Anne, Morrow, 3...|
|[juniper, 1986, B...|[[Jennifer, Littl...|
| [third, 18, Denver]| [[Todd, James, 27]]|
+--------------------+--------------------+

scala> almostHouseholdDF.printSchema
root
 |-- Address: struct (nullable = true)
 |    |-- street: string (nullable = true)
 |    |-- houseNumber: integer (nullable = false)
 |    |-- city: string (nullable = true)
 |-- People: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- firstName: string (nullable = true)
 |    |    |-- lastName: string (nullable = true)
 |    |    |-- age: integer (nullable = false)
```

## Retrieving Objects from Rows

### Get Item and Explode

We can undo what we did.  We can explode the People column to put each of the people back in their own row, and we can use `.getItem` to retrieve components of our objects.

```scala
val reFlattened = almostHouseholdDF
  .select(col("Address"), explode(col("People")) as "Person")
  .select(
    col("Person").getItem("firstName") as "fistName",
    col("Person").getItem("lastName") as "lastName",
    col("Person").getItem("age") as "age",
    col("Address").getItem("street") as "street",
    col("Address").getItem("houseNumber") as "houseNumber",
    col("Address").getItem("city") as "city"
  )
```


### User Defined Functions with Structured Input

We've seen how to use case classes to put data into interesting structures in a dataframe.  Using these structures again presents it's own problem.  Let's try to make a third column that shows the minimum age of the people in a household.

#### Here's something that won't work:
```scala
val minAge = udf((people: Seq[Person]) => people.map(_.age).min)
almostHouseholdDF.withColumn("minAge", minAge(col("People")))
```
Buried among the unleashed Spark fury is this error message: `Caused by: java.lang.ClassCastException: org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema cannot be cast to Person`

Except for simple types like `int` and `string`, user defined functions don't take inputs except for `Seq` and `Row`. `Seq` and `Row` are analogous to the array and dictionary parts of the json representation of an object, with `Row` a poor but ultimately effective substitute for dictionary.

#### Here's something else that won't work:
```scala
import org.apache.spark.sql.Row

val minAge = udf((people: Seq[Row]) => people.map(_.getItem("age")).min)
almostHouseholdDF.withColumn("minAge", minAge(col("People")))
```
Now our error is `value getItem is not a member of org.apache.spark.sql.Row`.  While `.getItem` worked for whole columns, it won't work for individual rows.

#### Here's something that will work:
```scala
val minAge = udf((people: Seq[Row]) => people.map(_.getInt(2)).min)
val minAgeDF = almostHouseholdDF.withColumn("minAge", minAge(col("People")))
```
```
+--------------------+--------------------+------+
|             Address|              People|minAge|
+--------------------+--------------------+------+
|[second, 28, Denver]|[[Anne, Morrow, 3...|     3|
|[juniper, 1986, B...|[[Jennifer, Littl...|     1|
| [third, 18, Denver]| [[Todd, James, 27]]|    27|
+--------------------+--------------------+------+
```

The Row type has all sorts of `.get` functions:  `.getInt`, `.getString`, etc.  The 2 came from the index of "age" in the structure.  The case class and the row are both in the order (firstName, lastName, age), and indexing starts at 0.  

Now we're going to try to filter our almostHouseholdDF so that it only includes people under 30.  This has nothing to do with museum membership costs, but it's surprisingly difficult, so we should figure out how to do it.  We still want an Address column and a People column with the same structure they have now, but if we subsequently explode the People column, we should have fewer rows than we started with in the original flat data.

#### Here's something that won't work:
```scala
val underThirty = udf((people: Seq[Row]) => people.filter(_.getInt(2) < 30))
val underThirtyDF = almostHouseholdDF
  .select(col("Addresses"), underThirty(col("People")) as "PeopleUnder30")
```

This gives us `java.lang.UnsupportedOperationException: Schema for type org.apache.spark.sql.Row is not supported`.  Despite the schema not being at all affected by a filter operation, Spark doesn't realize this and still looks around desperately for a case class to define the structure of the output; and as we saw earlier, when we tried to write a udf with a case class as an input, Spark has long forgotten about the case class that originally instantiated the column.

#### Here's something that will work:
```scala
val underThirty = udf((people: Seq[Row]) => people.filter(_.getInt(2) < 30)
  .map(row => Person(row.getString(0), row.getString(1), row.getInt(2))))
val underThirtyDF = almostHouseholdDF
  .select(col("Address"), underThirty(col("People")) as "PeopleUnder30")
```

#### Here's something else that will work:
```scala
val underThirty = udf((people: Seq[Row]) => people.map(row =>
  Person(row.getString(0), row.getString(1), row.getInt(2)))
  .filter(_.age < 30))
val underThirtyDF = almostHouseholdDF
  .select(col("Address"), underThirty(col("People")) as "PeopleUnder30")
```

## Companion Objects for Clean Row Constructors

In Scala, classes are often accompanied by objects with the same name as the class that hold static methods.  Let's revisit the case classes we defined initially, and this time let's give them static methods so that we can rebuild them from a spark row.

```scala
case class Person(firstName: String, lastName: String, age: Int)

object Person {
  def fromRow(row: Row): Person = {
    val firstName = row.getString(0)
    val lastName = row.getString(1)
    val age = row.getInt(2)
    Person(firstName, lastName, age)
  }
}

case class Address(street: String, houseNumber: Int, city: String)

object Address {
  def fromRow(row: Row): Address = {
    val street = row.getString(0)
    val houseNumber = row.getInt(1)
    val city = row.getString(2)
    Address(street, houseNumber, city)
  }
}
```

Let's try to underThirty filter again.
```scala
val underThirty = udf((people: Seq[Row]) =>
  people.map(Person.fromRow).filter(_.age < 30))
val underThirtyDF = almostHouseholdDF
  .select(col("Address"), underThirty(col("People")) as "PeopleUnder30")
```
Now the user defined function ends in a defined case class, and so Spark doesn't freak out about missing structure, and unlike the previous two (working) examples, we only have to remember the order of variables once, right next to where they were initially defined.

## Super Nested Household

Not to beat around the bush any more, we use the `.getStruct` method to pull out a Row object, and `.getSeq[A]` to get a sequence of type A, including type Row.  It's a bit inconsistent that `.getStruct` is not called `.getRow` since the returned object is a Row.  I can only guess as to the reasoning.

```scala
case class Household(members: Seq[Person], address: Address)

object Household {
  def fromRow(row: Row): Household = {
    val members = row.getSeq[Row](0).map(Person.fromRow)
    val address = Address.fromRow(row.getStruct(1))
    Household(members, address)
  }
}

val createHousehold = udf(
  (peopleRow: Seq[Row], addressRow: Row) =>
    Household(peopleRow.map(Person.fromRow), Address.fromRow(addressRow)
  )
)
val houseHoldDF = almostHouseholdDF
  .select(createHousehold(col("People"), col("Address")) as "Household")
```
```
+--------------------+
|           Household|
+--------------------+
|[[[Anne, Morrow, ...|
|[[[Jennifer, Litt...|
|[[[Todd, James, 2...|
+--------------------+
```

Now we're finally ready to find out how much each household should pay for museum membership, using the obviously made-up following functions.

```scala
object MuseumMembership {
  // top two pay full price, and additional members pay 50%
  // the household gets 10% off if they're in district
  def householdCost(household: Household): Double = {
    val individualCosts = household.members.map(individualCost).sorted
    val (discounted, topTwo) = individualCosts.splitAt(costs.length - 2)
    val outOfDistrictPrice = topTwo.sum + discounted.sum * .5
    if (inDistrict(household.address)) {
      outOfDistrictPrice * .9
    } else {
      outOfDistrictPrice
    }
  }

  def inDistrict(address: Address): Boolean = {
    val localStreets = Seq("first", "second", "third")
    val cityMatch = address.city == "Denver"
    val streetMatch = localStreets.contains(address.street)
    val numberMatch = address.houseNumber < 1000
    cityMatch & streetMatch & numberMatch
  }

  def individualCost(person: Person): Double = {
    val age = person.age
    if (age < 5) {
      3.0
    } else if (age < 18) {
      5.0
    } else if (age < 60) {
      10.50
    } else {
      7.0
    }
  }
}

val membershipCost = udf((household: Row) =>
  MuseumMembership.householdCost(Household.fromRow(household))
)

val result = houseHoldDF.withColumn("MembershipCost",
   membershipCost(col("Household")))
```
```
+--------------------+-----------------+
|           Household|   MembershipCost|
+--------------------+-----------------+
|[[[Anne, Morrow, ...|             21.6|
|[[[Jennifer, Litt...|             32.5|
|[[[Todd, James, 2...|9.450000000000001|
+--------------------+-----------------+
```
## Conclusion

When to use `.getSeq[A]` and when to use `.getStruct` are completely analogous to how json is constructed, and good organization means that there is some object with a `.fromRow` method waiting to be applied to the output of `.getStruct`.  I've found that having a `.fromRow` method for all case classes that I use in Spark allows me to continue using Object Oriented design while dipping into Spark when I need powerful parallel processing.
