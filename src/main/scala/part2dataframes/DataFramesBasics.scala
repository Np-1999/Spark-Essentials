package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {
  // Creating spark session
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true") // Never use inferSchema in prd operations cause it can be wrong
    .load("src/main/resources/data/cars.json")

  // DF ops
  // Showing DF
  firstDF.show()

  // PrintSchema
  firstDF.printSchema()

  // DF: Distributed collection of row(data) which conforms to a schema

  // get array of row i.e. every row as an array
  firstDF.take(10).foreach(println)

  // spark types
  val longType = LongType // Type for every variable type

  // schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDFSchema = firstDF.schema // Extracting schema from existing dataset
  println(carsDFSchema)

  // Read a DF with user defined schema

  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  carsDFWithSchema.show(20)

  // Create rows by hand

  val myRow = Row("chev chevelle malibu", 18,8,307,130,3504,12.0,"1970-01-01","USA") // Rows are unstructured

  //Create DF from Tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred cannot

  // Note: DFs have schema rows do not

  // Create DF with implicit
  import spark.implicits._
  val manualCarsDfWithImplicits = cars.toDF("Name","MPG","Cylinders","Displacement","HP","Weight","Acceleration","Year","CountryOrigin")

  manualCarsDF.printSchema()
  /* op
  root
   |-- _1: string (nullable = true)
   |-- _2: double (nullable = false)
   |-- _3: long (nullable = false)
   |-- _4: double (nullable = false)
   |-- _5: long (nullable = false)
   |-- _6: long (nullable = false)
   |-- _7: double (nullable = false)
   |-- _8: string (nullable = true)
   |-- _9: string (nullable = true)
   */
  manualCarsDfWithImplicits.printSchema()

  /* op
  root
   |-- Name: string (nullable = true)
   |-- MPG: double (nullable = false)
   |-- Cylinders: long (nullable = false)
   |-- Displacement: double (nullable = false)
   |-- HP: long (nullable = false)
   |-- Weight: long (nullable = false)
   |-- Acceleration: double (nullable = false)
   |-- Year: string (nullable = true)
   |-- CountryOrigin: string (nullable = true)
   */


  /*
  Exercises:
  1) Create a manual DF describing smartphones
    - make
    - model
    - screen dimension
    - camera megapixel
  2) Read another file from data/ folder eg. movies.json
    - print its schema
    - count the number of rows
   */
    println("******Exercise*******")
    val mobiles = Seq(
      ("Samsung","S22","1920x720","12"),
      ("Apple","iphone 12","1920x1440","12"),
      ("Samsung","S20 FE","1920x720","12")
    )
    val mDF = mobiles.toDF("make","model","screen dimension","camera megapixel")
    mDF.show()

  val moviesDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/movies.json")
  moviesDF.show()
  moviesDF.printSchema()
  println(moviesDF.count())
}
