/*
ОЧИСТКА
После очистки сразу же прочитайте топик - иначе он может не пересоздаться (см. Receiving messages ниже).
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper spark-node-1.newprolab.com:2181 --delete --topic vitaliy_belashov_lab04b_out

ЧТЕНИЕ
/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --bootstrap-server spark-master-1:6667 --topic vitaliy_belashov_lab04b_out

/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --bootstrap-server spark-master-1:6667 --from-beginning --topic vitaliy_belashov
/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --bootstrap-server spark-master-1:6667 --from-beginning --topic vitaliy_belashov_lab04b_out

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --class agg target/scala-2.11/agg_2.11-1.0.jar
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import java.time

object agg {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val schema = new StructType()
      .add("event_type", StringType, false)
      .add("category", StringType, false)
      .add("item_id", StringType, false)
      .add("item_price", StringType, false)
      .add("timestamp", StringType, false)
      .add("uid", StringType, false)

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "vitaliy_belashov")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .withColumn("jsonData",from_json(col("value"),schema))
      .select("jsonData.*")
      .withColumn("timestamp", ($"timestamp" / 1000).cast(TimestampType) )
      .withColumn(
        "purchases",
        when($"event_type" === "buy", lit(1)).otherwise(lit(0))
      )
      .withColumn(
        "revenue",
        when($"event_type" === "buy", $"item_price").otherwise(lit(0)).cast(IntegerType)
      )
      .groupBy(window($"timestamp", "1 hours")).agg(
        min($"timestamp").as("start_ts"),
        sum($"revenue").as("revenue"),
        count($"uid").as("visitors"),
        sum($"purchases").as("purchases")
      )
      .drop("window")
      .withColumn("start_ts", $"start_ts".cast(IntegerType))
      .withColumn("end_ts", $"start_ts" + 3600)
      .withColumn(
        "aov",
        when($"purchases" =!= 0, $"revenue" / $"purchases").otherwise(lit(0))
      )
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime("15" +
        " seconds"))
      .outputMode("update")
      .option("checkpointLocation", "cp/" + time.LocalDateTime.now().toString.replace(":","-"))
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", "vitaliy_belashov_lab04b_out")
      .start()
      .awaitTermination
  }
}
