package part7bigdata

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, from_unixtime, hour, lit, max, mean, min, not, round, stddev, sum, to_date, unix_timestamp}



object TaxiApplication extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Taxi big data application")
    .getOrCreate()

  import spark.implicits._
  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.printSchema()

  println(taxiDF.count())

  val taxiZonesDF = spark.read
    .option("header","true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  taxiZonesDF.printSchema()

  /*
  *  Questions:
  *  1. Which zones have the most pickups/dropoffs overall?
  *  2. What are the peak hours for taxi?
  *  3. How are the trips distributed by length? why are people taking the cab?
  *  4. What are peak hours for long/short trips?
  *  5. What are the top 3 pickup/dropoff zones for long/short trips?
  *  6. How are people paying for the ride, on long/short trips?
  *  7. How is the payment type evolving with time?
  *  8. Can we explore a ride-sharing opportunity by groupin close short trips?
  * */

  // 1

  val pickupsByTaxiZoneDF = taxiDF.groupBy("PULocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("totalTrips").desc_nulls_last)

  pickupsByTaxiZoneDF.show()

  //1 b
  val pickupsByBorough = pickupsByTaxiZoneDF.groupBy(col("Borough"))
    .agg(sum(col("totalTrips")).as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  pickupsByBorough.show()

  // 2
  val pickupsByHourDF = taxiDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  pickupsByHourDF.show()

  // 3
  val tripDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30
  val tripDistanceStatsDF = tripDistanceDF.select(
    count("*").as("count"),
    lit(longDistanceThreshold).as("threshold"),
    mean("distance").as("mean"),
    stddev("distance").as("stddev"),
    min("distance").as("min"),
    max("distance").as("max")
  )
  tripDistanceStatsDF.show()

  val tripsWithLengthDF = taxiDF
    .withColumn("isLong", col("trip_distance") >= longDistanceThreshold)

  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()
  tripsByLengthDF.show()

  // 4

  val longShorthrs= taxiDF
    .select( hour(col("tpep_pickup_datetime")).as("hour_of_day"),
      (col("trip_distance") >= longDistanceThreshold).as("isLong"))

  val pickupsByHourByLengthDF = longShorthrs.groupBy("hour_of_day" , "isLong")
    .agg(count("*").as("count")).orderBy(col("count").desc_nulls_last)
  pickupsByHourByLengthDF.show(48)

  // 5
  def pickupDropoffPopularity(predicate: Column) = tripsWithLengthDF
    .where(predicate)
    .groupBy("PULocationID", "DOLocationID").agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .withColumnRenamed("Zone","Pickup_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .withColumnRenamed("Zone","Dropoff_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .drop("PULocationID", "DOLocationID")
    .orderBy(col("totalTrips").desc_nulls_last)

  pickupDropoffPopularity(col("isLong")).show()
  pickupDropoffPopularity(not(col("isLong"))).show()

  // 6
  val ratecodeDistribution = taxiDF.
    groupBy(col("RatecodeID")).agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
  ratecodeDistribution.show()

  // 7
  val ratecodeEvolution = taxiDF
    .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RatecodeID"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("pickup_day"))


  // 8
  val groupAttemptsDF = taxiDF
    .select(round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId")
    , col("PULocationID"), col("total_amount"))
    .where(col("passenger_count") < 3)
    .groupBy(col("fiveMinId"), col("PULocationID"))
    .agg(count("*").as("total_trips"), sum(col("total_amount")).as("total_amount"))
    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") *300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID","service_zone")


}