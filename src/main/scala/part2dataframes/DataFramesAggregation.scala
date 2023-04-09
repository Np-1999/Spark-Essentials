package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, min, stddev, sum}

object DataFramesAggregation extends App{
  val spark = SparkSession.builder()
    .appName("Aggregation")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
     .option("inferSchema", "true")
     .json("src/main/resources/data/movies.json")


  // Counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // This does not count rows with null values for specified columns
  genresCountDF.show()
  // count with selectExpr
  moviesDF.selectExpr("count(Major_Genre)")
  moviesDF.select(count("*")).show() // count all the rows and will include null

  //counting distinct values
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  // withselectExpr
  moviesDF.selectExpr("min(IMDB_Rating)")

  //sum
  moviesDF.select(sum(col("US_Gross")))
  //with selectExpr
  moviesDF.selectExpr("sum(US_Gross)")

  //avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  //with selectExpr
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  //data science mean, stddev
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping
  val countByGenre = moviesDF.groupBy(col("Major_Genre")) // groupBy returns RelationalGroupedDataset
  countByGenre.count().show()

  // Grouping average
  val avgRatingByGenreDF = moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Rating")

  // AGG
  val aggregation = moviesDF
      .groupBy(col("Major_Genre"))
      .agg(
        count(col("*")).as("N_Movies"),
        avg(col("IMDB_Rating")).as("Avg_rating")
      ).orderBy("Avg_rating")

  aggregation.show()

  /*
    Exercise
    1. Sum up all the profits for all the movies in DF
    2. Count how many distinct directors
    3. Show the mean and standard deviation of us gross revenue for the movies
    4. Compute the average IMDB rating and the average US gross revenue per director
   */

  val sumProfit = moviesDF.select((col("Us_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))
                  .as("Total_Gross")).select(sum("Total_Gross"))
  sumProfit.show()

  val distinctDirectorsCount = moviesDF.select(countDistinct(col("Director")))
  distinctDirectorsCount.show()

  val usGrossDS = moviesDF.select(mean(col("US_Gross")), stddev(col("US_Gross")))
  usGrossDS.show()

  val directorRating = moviesDF
                  .groupBy("Director")
                  .agg(avg(col("IMDB_Rating")).as("Avg_Rating"),
                    sum(col("US_Gross"))
                  ).orderBy(col("Avg_Rating").desc_nulls_last)

  directorRating.show()

}
