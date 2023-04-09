package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object ManagingNulls extends App {
  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // select the first non null value
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  ).show()

  // Checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // remove nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows if value for any column is null

  // replace null
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating")).show()

  // fill with map
  moviesDF.na.fill( Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" ->10,
    "Director" -> "unknown"
  )).show()

  // Complex opearations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10)" , // Same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating *10 )", // samme as previous
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10)", // Null if both values are same otherwise first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0)" // if (first != null) then second else third
  ).show()


}
