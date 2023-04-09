package part3typesdatasets

import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object Datasets extends App{
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  // Convert a dataframe to a dataset
  numbersDF.printSchema()
  implicit  val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  numbersDS.filter(_ < 100)

  // dataset for complex type

  // 1 - Define your case class
  case class Car (
      Name: String,
      Miles_per_Gallon: Option[Double],
      Displacement: Option[Double],
      Horsepower: Option[Long],
      Weight_in_lbs: Option[Long],
      Acceleration: Option[Double],
      Year: String,
      Origin: String)

  // 2 - read the DF from the file
  val carsDF = spark.read.option("InferSchema", "true").json("src/main/resources/data/cars.json")
  // implicit  val carEncoder = Encoders.product[Car]

  // 3- Define an encoder
  import spark.implicits._
  // 4 - Convert Df to Ds
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_<100).show()

  // map, flatmap, fold, reduce, for comprehensions ...
  val carNameDS = carsDS.map(car => car.Name.toUpperCase())
  carNameDS.show()

  /*Exercises
  * 1. Count how many cars are there in dataframe
  * 2. Count how many powerful cars in DF (HP > 140)
  * 3. Avg Hp
  * */
  // 1
  val carsCount = carsDS.count()
  println(carsCount)
  // 2
  println(carsDS.filter( _.Horsepower.getOrElse(0L) > 140).count())
  // 3
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce( _ + _) / carsCount)

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitartype: String)
  case class GuitarPlayer(id: Long, name:String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = spark.read.option("inferSchema","true").json("src/main/resources/data/guitars.json").as[Guitar]
  val guitarPlayerDS = spark.read.option("inferSchema","true").json("src/main/resources/data/guitarPlayers.json")
    .as[GuitarPlayer]
  val bandsDS = spark.read.option("inferSchema","true").json("src/main/resources/data/bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayerDS.joinWith(
    bandsDS, guitarPlayerDS.col("band") === bandsDS.col("id"))

  guitarPlayerBandsDS.show()

  val guitarAndPlayers: Dataset[(GuitarPlayer, Guitar)] = guitarPlayerDS.joinWith(guitarsDS,
    array_contains(guitarPlayerDS.col("guitars"), guitarsDS.col("id")), "outer")

  // Grouping DS
 val carsGroupedByOrigin = carsDS
   .groupByKey(_.Origin)
   .count()
   .show()
}
