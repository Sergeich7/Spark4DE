// spark-submit --class lab02 target/scala-2.11/lab02_2.11-1.0.jar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.net.URLDecoder

object lab02 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("vitaliy_belashov_lab02")
      .getOrCreate()

    import spark.implicits._

    val autousersPath = "/labs/laba02/autousers.json"

    val usersSchemaJSON =
      StructType(
        List(
          StructField("autousers", ArrayType(StringType, containsNull = false))
        )
      )

    val users = spark.read.schema(usersSchemaJSON).json(autousersPath)
      .select(explode($"autousers")).toDF("u_uid")
      .withColumn("flag", lit(1))

    val logs_path = "/labs/laba02/logs"
    val domains = spark.read.option("delimiter", "\t").csv(logs_path)
      .drop("_c1")
      .filter($"_c1".isNotNull)
      .filter($"_c0".isNotNull)
      .map(row => {
          var url = row.getString(1)
          try {
            url = URLDecoder.decode(url, "UTF-8")
          } catch {
            case e: Exception => url = "_"
          }
          (row.getString(0), url)
        }
      )
      .where(not($"_2" === '_')).toDF()
      .map(row => {
          val url = row.getString(1)
            .replace(" ", "").replace("\t", "")
            .replace("%3A", ":").replace("%2F", "/")
            .replace("https://", "http://").replace("http://www.", "http://")
          (row.getString(0), url)
        }
      )
      .filter(col("_2").contains("http://")).toDF()
      .map(row => {
          val domain = row.getString(1).split("/")(2)
          (row.getString(0), domain)
        }
      )
      .filter(col("_2").contains(".")).toDF("d_uid", "domain")

    val joined = domains.join(broadcast(users), $"d_uid" === $"u_uid", "left_outer")
      .drop("u_uid", "d_uid").groupBy($"domain")
      .agg(
        count("*").as("count"),
        sum("flag").as("users")
      )
      .filter($"users".isNotNull)

    val visiter_by_users = joined.agg(sum("users").cast("long")).first.getLong(0)

    val relevance = joined
      .map(row => {
          val domain = row.getString(0)
          val count = row.getLong(1)
          val sum = row.getLong(2)
          val rel: Float = sum * sum.toFloat / (count * visiter_by_users)
          (domain, rel, sum, count)
        }
      )
      .withColumn("_2", format_number($"_2", 15))
      .coalesce(1)
      .sort(col("_2").desc, col("_1").asc)
      .limit(200)
      .toDF("domain", "relevance", "sum", "count")

    val result_path = "lab02_result"
    relevance.drop("sum", "count")
      .write.mode("overwrite").option("delimiter", "\t").csv(result_path)


    println("All done.")
  }
}
