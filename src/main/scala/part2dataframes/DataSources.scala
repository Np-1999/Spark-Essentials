package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master","local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
    - Format
    - Schema or inferSchema = true
    - Zero or more options
   */

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode","permissive") // dropMalformed, permissive (default)
    .load("src/main/resources/data/cars.json")

  val carsDFO = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path","src/main/resources/data/cars.json")
    .load()

  carsDF.show()
  carsDF.printSchema()

  // Option map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options( Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    )) // Allows to compute dimensions dynamically
    .load()

  /* writing DF
   - Format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
  */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dup.json")

  // JSON flags
  val jsonReader = spark.read
    .schema(carsSchema)
    .options(Map(
      "dateFormat" -> "yyyy-mm-dd",  // If DateType columns does not match this schema then spark will parse it as NULL
      "allowSingleQuotes" -> "true",
      "compression" -> "uncompressed" // bzip2, gzip, lz4, snappy, deflate
    )).json("src/main/resources/data/cars.json")

  jsonReader.printSchema()


  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .options(Map(
      "dateFormat" -> "mmm dd yyyy",
      "header" -> "true",
      "sep" -> ",",
      "nullValue" -> ""
    )).csv("src/main/resources/data/stocks.csv")

  // Parquet : Default storage format for spark
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  val employessDF = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.employees"
    )).load()

  employessDF.show()
  /*
    Exercise: read movies DF, then write as
     - tab-separated values file
     - Parquet file
     - table "public.movies" in postgres db
   */
  /*
  val moviesSchema = StructType(Array(
    StructField("Title", StringType),
    StructField("US_Gross", DoubleType),
    StructField("Worldwide_Gross", DoubleType),
    StructField("US_DVD_Sales",DoubleType),
    StructField("Production_Budget",DoubleType),
    StructField("Release_Date", DateType),
    StructField("MPAA_Rating", StringType),
    StructField("Running_Time_min",DoubleType),
    StructField("Distributor", StringType),
    StructField("Source", StringType),
    StructField("Major_Genre", StringType),
    StructField("Creative_Type", StringType),
    StructField("Director", StringType),
    StructField("Rotten_Tomatoes_Rating", DoubleType),
    StructField("IMDB_Rating", DoubleType),
    StructField("IMDB_Votes", DoubleType)
  ))*/
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  moviesDF.show()

  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "separator" ->"\t",
      "header" -> "true"
    )).save("src/main/resources/data/movies.csv")

  moviesDF.write.mode(SaveMode.Overwrite).save("src/main/resources/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .options(Map(
      "Driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.movies"
    )).save()
}
