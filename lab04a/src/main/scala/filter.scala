// spark-submit --conf spark.filter.topic_name=lab04_input_data --conf spark.filter.offset=earliest --conf spark.filter.output_dir_prefix=/user/vitaliy.belashov/visits --class filter --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 ./target/scala-2.11/filter_2.11-1.0.jar
// spark-submit --master local[1] --conf spark.filter.topic_name=lab04_input_data --conf spark.filter.offset=180540 --conf spark.filter.output_dir_prefix=file:///data/home/vitaliy.belashov/v3 --class filter --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 ./target/scala-2.11/filter_2.11-1.0.jar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object filter {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    /*
     read params --conf
     */

    val param_topic_name: String =
      spark.sqlContext.getConf("spark.filter.topic_name", "lab04_input_data")

    val param_offset: String =
      spark.sqlContext.getConf("spark.filter.offset", "earliest")

    val param_output_dir_prefix: String =
      spark.sqlContext.getConf("spark.filter.output_dir_prefix", "/user/vitaliy.belashov/visits")

    /*
    read kafka topic
    */
    val schema = new StructType()
      .add("event_type", StringType, false)
      .add("category", StringType, false)
      .add("item_id", StringType, false)
      .add("item_price", StringType, false)
      .add("timestamp", StringType, false)
      .add("uid", StringType, false)
      .add("date", StringType, false)
      .add("p_date", StringType, false)

    val input_data = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", param_topic_name)
      .option("startingOffsets",
        if(param_offset.contains("earliest"))
          param_offset
        else {
          "{\"" + param_topic_name + "\":{\"0\":" + param_offset + "}}"
        }
      )
      .load()
      .selectExpr("CAST(value AS STRING)")
      .withColumn("jsonData",from_json(col("value"),schema))
      .select("jsonData.*")
      .withColumn("date", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))
      .withColumn("p_date", col("date"))
//      .cache()

    val count_kafka_records = input_data.count()

    /*
    save sorted result
    */
    List("view", "buy").foreach(et => {
        input_data.where($"event_type" === et)
          .write
          .partitionBy("p_date")
          .mode("overwrite")
          .json(param_output_dir_prefix + "/" + et)
      }
    )

    println("All done...\n"
      + param_offset + "\n"
      + param_output_dir_prefix + "\n"
      + param_topic_name+ "\n"
      + count_kafka_records
    )
  }
}
