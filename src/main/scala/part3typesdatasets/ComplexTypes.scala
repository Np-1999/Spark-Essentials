package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, current_date, current_timestamp, datediff, expr, size, split, struct, to_date}

object ComplexTypes extends App{
  val spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDF
    .select(col("Title"), col("Release_Date") , to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))

  val firstDatePass = moviesWithReleaseDates.where(col("Actual_Release").isNotNull)
  // If to_date function fails to parse a value it will return null

  moviesWithReleaseDates
    .select(col("Title"), col("Actual_Release"), current_date().alias("Today"), current_timestamp().as("Right_Now")
    ).withColumn("Movie_Age", (datediff(col("Today"), col("Actual_Release")) / 365))
    .show()

  /**
    * Exercise
    *  1. Deal with multiple date formates? Approach will be different for spark 3
    *  2. Read the stocks DF and parse the date formats
    * */
  val moviesWithReleaseDateNulls = moviesWithReleaseDates.where(col("Actual_Release").isNull)
  val dateParseTwo = moviesWithReleaseDateNulls.select(col("Title"), col("Release_Date"),to_date(col("Release_Date"),"yyyy-MM-dd").as("Actual_Release"))
  val finalDf = firstDatePass.union(dateParseTwo)
  finalDf.where(col("Actual_Release").isNull).show()

  val stocks = spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/stocks.csv")

  val stocks_date = stocks.select(col("symbol"), to_date(col("date"),"MMM dd yyyy"),col("price"))
  stocks_date.show()

  // Structures
  // 1 - with col operators
  moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit")).show()

  // Structures are tuples

  // 2 - with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross").show()

  // Arrays
  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title")," |,").as("Title_Words"))

  // Array functions
  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()


}
