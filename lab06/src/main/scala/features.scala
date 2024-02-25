object features {
  import org.apache.spark.sql.{DataFrame, SparkSession}
  //  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._
  //    import org.apache.spark.sql.Row


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName(name = "Vitaliy Belashov lab06")
      .config("spark.driver.cores", 1)
      .config("spark.driver.memory", "4g")
      .config("spark.driver.maxResultSize", "1g")
      .config("spark.executor.instances", 5)
      .config("spark.executor.cores", 2)
      .config("spark.executor.memory", "4g")
      .config("spark.default.parallelism", 10)
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val visited_domains: DataFrame = spark.read
      .json("hdfs:///labs/laba03/weblogs.json")
      .select($"uid",explode($"visits"))
      .select($"uid", $"col.timestamp", $"col.url")
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .drop("url", "host")
      .withColumn("timestamp", from_unixtime($"timestamp" / 1000))

    val uids = visited_domains.select($"uid")
      .groupBy($"uid").count()
      .drop("count")

    var top1000domains = visited_domains.select($"domain")
      .filter($"domain" =!= "null")
      .groupBy("domain").count()
      .orderBy($"count".desc)
      .limit(1000)
      .orderBy($"domain")

    val uidsXtop1000domains = uids.crossJoin(top1000domains)

    top1000domains = top1000domains.withColumn("in_top_1000", lit(1))

    val top1000visits = visited_domains
      .join(broadcast(top1000domains), Seq("domain"), "left_outer")
      .filter($"in_top_1000" === 1)
      .groupBy($"uid", $"domain").agg(sum($"in_top_1000").alias("visits"))

    val domain_features = uidsXtop1000domains
      .join(top1000visits, Seq("domain", "uid"), "left_outer")
      .na.fill(0)
      .groupBy("uid").pivot("domain").sum("visits")
      .map(row => {
          var array = new Array[Int](1000)
          for (i <- 1 to 1000){
            array(i-1) =
              try { row.getLong(i).toInt }
              catch { case e: Exception => 0 }
          }
          (row.getString(0), array)
        }
      )
      .toDF("uid","domain_features")

    val web_day = visited_domains
      .withColumn("web_day", concat(lit("web_day_"), lower(date_format($"timestamp", "E"))))
      .groupBy("uid").pivot("web_day").count()
      .na.fill(0)

    val web_hour = visited_domains
      .withColumn("web_hour", concat(lit("web_hour_"), hour($"timestamp")))
      .groupBy("uid").pivot("web_hour").count()
      .na.fill(0)

    val web_count = visited_domains.groupBy("uid").count()

    val web_rank_hour = visited_domains
      .withColumn("hour", hour($"timestamp"))
      .withColumn("web_rank_hour",
        when( $"hour" >= 9 and $"hour" < 18, lit("web_work_hours"))
          .otherwise(
            when($"hour" >= 18 and $"hour" < 24, lit("web_evening_hours"))
              .otherwise(lit("web_night_hours"))
          )
      )
      .groupBy("uid").pivot("web_rank_hour").count()
      .drop("web_night_hours")
      .na.fill(0)
      .join(web_count, Seq("uid"), "inner")
      .withColumn("web_fraction_work_hours", $"web_work_hours" / $"count")
      .drop("web_work_hours")
      .withColumn("web_fraction_evening_hours", $"web_evening_hours" / $"count")
      .drop("web_evening_hours", "count")

    val features = spark.read.parquet("users-items/20200429")
      .join(domain_features, Seq("uid"), "inner")
      .join(web_day, Seq("uid"), "inner")
      .join(web_hour, Seq("uid"), "inner")
      .join(web_rank_hour, Seq("uid"), "inner")

    features.write.mode("overwrite").parquet("features" )

    println("All data saved...")
  }
}
