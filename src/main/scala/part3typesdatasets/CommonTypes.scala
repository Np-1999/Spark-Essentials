package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, lit, not, regexp_extract, regexp_replace}

object CommonTypes extends App{
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  // Add column without withColumn

  moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

  // Booleans
  val dramaFilter = col("Major_Genre") === "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  val moviesWithGoodnessFlagDF  = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  // Filter on boolean column
  moviesWithGoodnessFlagDF.where(col("good_movie")).show() // where (col("good_movie") === "true")
  // negation
  moviesWithGoodnessFlagDF.where(not(col("good_movie")))

  // Numbers
  // Math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"),(col("Rotten_Tomatoes_Rating")/ 10 + col("IMDB_Rating"))/2)

  // correlation
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating","IMDB_Rating")) // corr is an action

  // Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name"))).show()

  // Contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  //regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")
  vwDF.show()

  //replace
   vwDF.select(
     col("Name"),
     regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
   ).show()

  /* Exercises
  *
  * Filter the cars DF by a list of car names obtained by an API Call
  * - contains
  *  - regexes
   */
  def getCarName(): List[String] = {
    List("volkswagen","ford", "toyota")
  }
  val regexLong = getCarName().mkString("|")
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexLong, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract").show()

  val carNameFilters = getCarName().map(name =>col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter)=> combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()

}
