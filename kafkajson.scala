import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{col, from_json}

object kafkajson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("kafkajson").master("local[2]").getOrCreate()
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .option("startingOffsets", "earliest")
      .load() // castear los datos leidos en formato kafka para convertirlos en strings
    val res = df.selectExpr("CAST(value AS STRING)")
    val schema = new StructType()
      .add("id",IntegerType)
      .add("first_name", StringType)
      .add("last_name", StringType)
      .add("gender", StringType)
      .add("email", StringType)
      .add("ip_address", StringType)


    val personal = res.select(from_json(col("value"), schema).as("data"))
      .select("data.*")
    val filter_df=personal.filter("first_name !='Willard' and first_name !='Noell'")



    print("mostrar los datos por consola")
    filter_df.writeStream

      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
