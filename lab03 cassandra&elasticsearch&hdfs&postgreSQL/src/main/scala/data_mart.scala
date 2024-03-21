//spark-submit --packages org.elasticsearch:elasticsearch-spark-20_2.11:6.8.22,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3,org.postgresql:postgresql:42.3.3 --class data_mart target/scala-2.11/data_mart_2.11-1.0.jar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.net.URLDecoder

object data_mart {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .config("spark.cassandra.connection.host", "XXXXXXX")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

    import spark.implicits._

    val clients: DataFrame = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()
      .map(el => {
          val uid = el.getString(0)
          val age = el.getInt(1)
          val gender = el.getString(2)
          val age_cat = age match {
            case a if (a >= 18 && a <= 24) => "18-24"
            case a if (a >= 25 && a <= 34) => "25-34"
            case a if (a >= 35 && a <= 44) => "35-44"
            case a if (a >= 45 && a <= 54) => "45-54"
            case _ => ">=55"
          }
          (uid, age_cat, gender)
        }
      )
      .toDF("uid","age_cat","gender")

    val visits_shop_cat: DataFrame = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.read.metadata" -> "true",
          "es.nodes.wan.only" -> "true",
          "es.port" -> "9200",
          "es.nodes" -> "XXXXXXX",
          "es.net.ssl" -> "false"
        )
      )
      .load("visits")
      .filter($"uid".isNotNull)
      .filter($"category".isNotNull)
      .drop("event_type", "_metadata", "item_id","item_price","timestamp")
      .map(row => {
          val category = row.getString(0)
            .replace(" ", "_")
            .replace("-", "_")
            .toLowerCase()
          val uid = row.getString(1)
          (uid, "shop_" + category)
        }
      )
      .toDF("uid_s","shop_cat")
      .groupBy("uid_s")
      .pivot("shop_cat")
      .count()

    val logs: DataFrame = spark.read
      .json("hdfs:///labs/laba03/weblogs.json")
      .filter($"uid".isNotNull)
      .filter($"visits".isNotNull)
      .select($"uid",explode($"visits"))
      .select($"uid", $"col.url")
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
            .toLowerCase()
            .replace(" ", "").replace("\t", "")
            .replace("%3A", ":").replace("%2F", "/")
            .replace("https://", "http://").replace("http://www.", "http://")
          (row.getString(0),url)
        }
      )
      .filter(col("_2").contains("http://")).toDF()
      .map(row => {
          val domain = row.getString(1).split("/")(2)
          (row.getString(0), domain)
        }
      )
      .filter(col("_2").contains("."))
      .toDF("uid_w", "domain")

    val cats: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://XXXXXXX:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "vitaliy_belashov")
      .option("password", "XXXXXXX")
      .option("driver", "org.postgresql.Driver")
      .load()
      .filter($"domain".isNotNull)
      .filter($"category".isNotNull)
      .filter(col("domain").contains("."))
      .map(row => {
          val category = row.getString(1)
            .replace(" ", "_")
            .replace("-", "_")
            .toLowerCase()
          val domain= row.getString(0)
          (domain, "web_" + category)
        }
      )
      .toDF("domain", "web_cat")

    val visits_web_cat = logs.join(cats, logs("domain") === cats("domain"), "left_outer")
      .drop("domain", "domain")
      .filter($"web_cat".isNotNull)
      .groupBy("uid_w")
      .pivot("web_cat")
      .count()

    val result = clients
      .join(visits_shop_cat, $"uid" === $"uid_s", "left_outer")
      .join(visits_web_cat, $"uid" === $"uid_w", "left_outer")
      .drop("uid_s", "uid_w")

    result.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://XXXXXXX:5432/vitaliy_belashov")
      .option("dbtable", "clients")
      .option("user", "vitaliy_belashov")
      .option("password", "XXXXXXX")
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true) //позволит не терять гранты на таблицу
      .mode("overwrite") //очищает данные в таблице перед записью
      .save()

    result.filter($"uid" === "d50192e5-c44e-4ae8-ae7a-7cfe67c8b777").show(false)
    clients.groupBy("gender", "age_cat").count().orderBy("gender", "age_cat").show()
    val rc = result.count()
    println(rc)

  }

  /*
    import org.postgresql.Driver
    import java.sql.{Connection, DriverManager, Statement}

    def grantTable(): Unit = {
      val driverClass: Class[Driver] = classOf[org.postgresql.Driver]
      val driver: Any = Class.forName("org.postgresql.Driver").newInstance()
      val url = "jdbc:postgresql://XXXXXXX:5432/vitaliy_belashov?user=vitaliy_belashov&password=XXXXXXX"
      val connection: Connection = DriverManager.getConnection(url)
      val statement: Statement = connection.createStatement()
      val bool: Boolean = statement.execute("GRANT SELECT ON clients TO labchecker2")
      connection.close()
    }
    grantTable()
   */
}
