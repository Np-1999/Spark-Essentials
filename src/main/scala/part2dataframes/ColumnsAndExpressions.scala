package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
object ColumnsAndExpressions extends App{

  val spark = SparkSession.builder()
    .appName("DF columns and Expression")
    .config("spark.master","local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Column
  val firstColumn = carsDF.col("Name") // A generic column it does not contain any data. Column objects can be composed to form complex expression
  // Selecting(projecting)
  val carNamesDF = carsDF.select(firstColumn)
  carNamesDF.show()

  //Various ways to select
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // scala symbol, auto-converted to column
    $"Horsepower",
    expr("Origin") //pronounced as expra
  )
  carsDF.select("Name", "Year")

  //EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weighetInKgExpression = simplestExpression / 2.2
  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    simplestExpression,
    weighetInKgExpression.as("Weight_in_kg")
  )
  carsWithWeightsDF.show()

  val carsWithExpr = carsDF.select(
    col("Name"),
    simplestExpression,
    weighetInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs/2.2").as("Weight_in_kg")
  )
  carsWithExpr.show()

  val carsWithSelectExprWeightsDf = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2 as Weight_in_kg"
  )

  carsWithSelectExprWeightsDf.show()

  // DF processing
  // Adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3",col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // Handling columns with dash and space in name
  carsWithColumnRenamed.selectExpr("`Weight in pounds`").show() // notice the column name is enclosed in backtick
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  //filtering
  val notUSACars = carsDF.filter(col("Origin") =!= "USA") // to use equal use ===
  val notUSACars2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with exprssion strings
  val americanCarsDf = carsDF.filter("Origin = 'USA' ")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)

  // Unioning = adding more rows

  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)

  // Distinct
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()
  /*
    Exercises
    1. Read movies DF and select 2 columns
    2. Create another column summing up the total profit
    3. Select comedy movies with IMDB > 6
   */

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  val select2 = moviesDF.select(col("Title"), $"US_Gross")
  select2.show()

  val totalProfit = moviesDF.select(col("Title"),expr("US_Gross + Worldwide_Gross ").as("Total_Gross"))
  totalProfit.show()

  val comedyMovies = moviesDF.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating")> 6).select('Title)
  comedyMovies.show()

}
