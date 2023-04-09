package part5lowlevel

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App {
  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("RDDs")
    .getOrCreate()

  val sc = spark.sparkContext

  // 1 - Parallelize an existing collection
  val numbers = 1 to 10000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date:String, price:Double)
  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1) //Drop header
      .map(line=> line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))
  println(stocksRDD.count())

  //2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv") // returns RDD of string. Each line is whole string
    .map(line => line.split(","))
    .filter(token => token(0) == token(0).toUpperCase())
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 Reading from DF
  val stocksDF = spark.read.option("inferSchema", "true").option("header","true").csv("src/main/resources/data/stocks.csv")
  import spark.implicits._
  val stockRDD = stocksDF.as[StockValue].rdd

  // RDD to DF
  val numbersDF = numbersRDD.toDF("numbers") // Will loose type information

  // RDD to DS
  val numbersDS: Dataset[Int] = spark.createDataset(numbersRDD)

  // Transformations
  // filter
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT")
  // count
  msftRDD.count()
  //distinct
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct()

  // min and max
  implicit  val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan((sa: StockValue, sb:StockValue) => sa.price < sb.price )

  val minMsft = msftRDD.min() // Need to define ordering firs

  //reduce
  numbersRDD.reduce(_ + _)

  // Grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)

  // Partitoning
  val repartitionStocksRDD = stocksRDD.repartition(30)
  repartitionStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  // coalesce
  val coalescedRDD = repartitionStocksRDD.coalesce(15) // Reduce number partition not a true shuffle
  coalescedRDD.toDF().write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/stock15")


  /*Exercises
  * 1. Read the movies.json as an RDD
  * 2. Show the distinct genres as an RDD.
  * 3. Select all the movies in the Dram genre with IMDB rating > 6
  * 4. Show the average rating of movies by genre
  * */

  case class Movie(title: String, genre: String, rating: Double )

  val moviesDF = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")
  val requiredDS = moviesDF.select(
      col("title"),
      col("Major_Genre") as "genre" ,
      col("IMDB_rating") as "rating"
    ).where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]

  val moviesRDD = requiredDS.rdd

  moviesRDD.map(_.genre).distinct().toDF().show()

  moviesRDD.filter(movie => movie.genre =="Drama" && movie.rating > 6.0).toDF().show()

  case class GenreAvgRating(genre: String, rating: Double)
  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map{
    case(genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum/ movies.size)
  }
  avgRatingByGenreRDD.toDF().show()

}
