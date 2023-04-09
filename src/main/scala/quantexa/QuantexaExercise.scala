package quantexa

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object QuantexaExercise extends App{
  val spark = SparkSession.builder().master("local[*]").appName("CustomerAddress").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]

  import spark.implicits._

  //Set logger level to Warn


  case class AddressRawData(
                             addressId: String,
                             customerId: String,
                             address: String
                           )

  case class AddressData(
                          addressId: String,
                          customerId: String,
                          address: String,
                          number: Option[Int], //i.e. it is optional
                          road: Option[String],
                          city: Option[String],
                          country: Option[String]
                        )

  // Obvious assumption is we need to parse AddressRawData as AddressRawData



  val addressDS: Dataset[AddressRawData] = spark.read.option("header", "true").csv("src/main/resources/data/address_data.csv").as[AddressRawData]
  addressDS.show()

  def addressDataModify(addressRawData: AddressRawData): AddressData = {
    val addressSplit: Array[String] = addressRawData.address.split(",")

    AddressData(addressRawData.addressId, addressRawData.customerId, addressRawData.address,Some(addressSplit(0).toInt), Some(addressSplit(1)), Some(addressSplit(2)), Some(addressSplit(3)))
  }
  val addressDataDS = addressDS.map( address => addressDataModify(address))
  addressDataDS.show()
  // val customerDF: Dataset[CustomerData] = spark.read.option("header", "true").csv("src/main/resources/customer_data.csv").as[CustomerData]
  // val addressDS = addressDF.withColumn("number", lit(null)).withColumn("road", lit(null)).withColumn("city", lit(null)).withColumn("country", lit(null)).as[AddressData]
  // val addressSeq: Seq[AddressData] = addressDS.collect().toSeq
  // val finaladdressDS = addressParser(addressSeq)



  /*val addressDFWithcolumns = addressDF.withColumn("number",null).withColumn("road", null).withColumn("city", null).withColumn("country", null)
  val addressDS = addressDFWithcolumns.as[AddressData]
  val addressDataSeq = addressDS.collect().toSeq
  val addressDataModifiedSeq = addressParser(addressDataSeq)
  println(addressDataModifiedSeq)*/


  //  val customerAccountDS = spark.read.parquet("src/main/resources/customerAccountOutputDS.parquet").as[CustomerAccountOutput]

  //END GIVEN CODE
}
