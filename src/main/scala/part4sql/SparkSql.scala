package part4sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import part2dataframes.DataFramesJoins.spark

object SparkSql extends App{
  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.master", "local")
    .config("spark.sql.catalogImplementation","in-memory")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDf = spark.read.option("inferSchema","true").json("src/main/resources/data/cars.json")
  carsDf.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )

  americanCarsDF.show()
  spark.sql("create database rtjvm") // returns empty df
  val databasesDF = spark.sql("show databases")
  databasesDF.show()

  def readTable(tableName: String) : DataFrame ={
    spark.read
      .format("jdbc")
      .options(Map(
        "driver" -> "org.postgresql.Driver",
        "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
        "user" -> "docker",
        "password" -> "docker",
        "dbtable" -> s"public.$tableName",
        "inferSchema" -> "true"
      )).load()
  }
  def transferTables(tableNames: List[String]) = tableNames.foreach {
    tableName =>
      val tableDF = readTable(tableName)
      tableDF.createOrReplaceTempView(tableName)
      spark.sql(s"drop table $tableName")
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
  }
  // transfer tables from a DB to Spark table
  spark.sql("use rtjvm")
  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager"
  ))

  /*
  * Exercises:
  * 1. Read the movies DF and store it as a Spark table in the rtjvm database
  * 2. Count how many employees were hired in between Jan 1 2000 and Jan 1 2001.
  * 3. Show the average salaries for the employees hired in between those dates, grouped by department
  * 4. Show the name of the best paying department for employees hired in between those dates.
  * */

  val moviesDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  moviesDf.write.saveAsTable("Movies")
  spark.sql(
    """
      |SELECT COUNT(*) FROM employees WHERE hire_date between DATE('1999-01-01') AND DATE('2001-01-01')
      |""".stripMargin).show()
  spark.sql(
    """
      |SELECT d.dept_no, avg(s.salary) FROM employees as e
      |LEFT JOIN salaries as S ON s.emp_no = e.emp_no
      |LEFT JOIN dept_emp as D on D.emp_no = e.emp_no
      |GROUP BY d.dept_no
      |""".stripMargin
  ).show()

  spark.sql(
    """
      |SELECT dept_name FROM departments WHERE dept_no IN(
      |SELECT dept_no FROM (
      |SELECT d.dept_no, sum(s.salary) as salary  FROM employees as e
      |LEFT JOIN salaries as S ON s.emp_no = e.emp_no
      |LEFT JOIN dept_emp as D on D.emp_no = e.emp_no
      |GROUP BY d.dept_no ORDER BY salary desc limit 1))
      |""".stripMargin
  ).show()
}
