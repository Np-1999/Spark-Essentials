package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object DataFramesJoins extends App {
  val spark = SparkSession.builder()
      .appName("Dataframe joins")
      .config("spark.master", "local")
      .getOrCreate()

  val guitarsDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")
  val guitaristDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")
  val bandsDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")

  // joins
  val joinCondition = bandsDf.col("id") === guitaristDf.col("band")
  val guitaristBandsDf = guitaristDf.join(bandsDf)

  // outer joins
  guitaristDf.join(bandsDf, joinCondition, "left_outer").show()
  guitaristDf.join(bandsDf, joinCondition, "right_outer").show()

  // Outer join = everything in the inner join + all the rows in both tables, with nulls in where the data is missing
  guitaristDf.join(bandsDf, joinCondition, "outer").show()

  // semi joins
  guitaristDf.join(bandsDf, joinCondition, "left_semi").show()

  //anti-joins
  guitaristDf.join(bandsDf, joinCondition, "left_anti").show()

  // handling duplicate column

  //option 1 - rename the column on which we are joining
  guitaristDf.join(bandsDf.withColumnRenamed("id","band"), "band").show()

  // option 2 - drop dupes column
  guitaristBandsDf.drop(bandsDf.col("id"))

  // Option 3- Rename duplicate column
  val bandsModDF = bandsDf.withColumnRenamed("id","bandId")
  guitaristDf.join(bandsModDF, guitaristDf.col("band") === bandsModDF.col("bandId"))

  // Complex type
  guitaristDf.join(guitarsDf.withColumnRenamed("id","guitarId"), expr("array_contains(guitars, guitarId)")).show()

  /* Exercises
    - Show all employees and their max salary
    - Show all employees who were never managers
    - find the job titles of the best paid 10 employees in the company
   */
  val employessDF = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.employees",
      "inferSchema" -> "true"
    )).load()

  val salariesDF = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.salaries",
      "inferSchema" -> "true"
    )).load()

  val aggregatedDF = salariesDF.groupBy(col("emp_no")).agg(max("salary").as("max_salary"))

  //first execise solution
  employessDF.join(aggregatedDF, employessDF.col("emp_no") === salariesDF.col("emp_no")).show()

  val managerDF = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.dept_manager",
      "inferSchema" -> "true"
    )).load()



  // 2nd solution
  employessDF.join(managerDF, employessDF.col("emp_no") === managerDF.col("emp_no"), "left_anti").show()


  val titlesDF = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.titles",
      "inferSchema" -> "true"
    )).load()
  val exerciseStart = employessDF.join(aggregatedDF, employessDF.col("emp_no") === salariesDF.col("emp_no"))
  val cleanedDf = exerciseStart.drop(salariesDF.col("emp_no")).orderBy(col("max_salary").desc)
  cleanedDf.show()
  val top10employees = cleanedDf.limit(10)
  val titlesAggregated = titlesDF.groupBy("emp_no").agg(expr("max_by(title, to_date)"))
  top10employees.join(titlesAggregated, top10employees.col("emp_no") === titlesAggregated.col("emp_no")).orderBy(col("max_salary").desc).show()



}