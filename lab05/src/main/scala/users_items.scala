/*
spark-submit --class users_items target/scala-2.11/users_items_2.11-1.0.jar

spark-submit --master local[4] --conf spark.users_items.input_dir=file:///data/home/vitaliy.belashov/lab05/visits --conf spark.users_items.output_dir=file:///data/home/vitaliy.belashov/lab05/ui --conf spark.users_items.update=1 --class users_items target/scala-2.11/users_items_2.11-1.0.jar


*/

object users_items {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.{DataFrame, SparkSession}
//    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
//    import org.apache.spark.sql.Row

    val spark: SparkSession = SparkSession.builder()
      .appName(name = "Vitaliy Belashov lab05")
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

    /*
  READ CONF PARAMETERS

  spark.users_items.input_dir=/user/name.surname/visits
  spark.users_items.output_dir=/user/name.surname/users-items
  spark.users_items.update=0
  */

    val param_input_dir: String =
      spark.sqlContext.getConf("spark.users_items.input_dir", "/user/vitaliy.belashov/visits")

    val param_output_dir: String =
      spark.sqlContext.getConf("spark.users_items.output_dir", "/user/vitaliy.belashov/users-items")

    var param_update: String =
      spark.sqlContext.getConf("spark.users_items.update", "1" )

    val input_df: DataFrame = spark
      .read
      .json(param_input_dir + "/view")
      .union(
        spark.read.json(param_input_dir + "/buy")
      )
      .filter($"uid".isNotNull)
      .filter($"item_id".isNotNull)
      .filter($"event_type".isNotNull)
      .drop("category", "item_price","timestamp","p_date")

    val max_date = input_df.select($"date").distinct().orderBy($"date".desc).head()(0).toString

    var output_df = input_df.drop("date")
      .withColumn("item_cat", lower(concat($"event_type", lit("_"), $"item_id")))
      .withColumn("item_cat", regexp_replace($"item_cat", "-", "_"))
      .withColumn("item_cat", regexp_replace($"item_cat", " ", "_"))
      .drop("event_type", "item_id")
      .groupBy("uid").pivot("item_cat").count()
      .na.fill(0)

    if (param_update == "1"){

      var prev_date: String =
        try {
          spark.read.parquet(param_output_dir + "/*")
            .withColumn("file_name",input_file_name())
            .select("file_name")
            .distinct
            .withColumn("file_name", split( $"file_name", "/"))
            .withColumn("file_name", $"file_name"(size($"file_name")-2))
            .filter($"file_name" =!= max_date)
            .agg(max($"file_name"))
            .head()(0).toString
        }
        catch { case e: Exception => "" }

      if(prev_date.length != 8){
        println("No previous data")
      }
      else{
        val prev_df = spark.read.parquet(param_output_dir + "/" + prev_date)

        /*
        MERGE WITH PREVIOS DATA
        */
        val merged_cols = output_df.columns.toSet ++ prev_df.columns.toSet

        def getNewColumns(column: Set[String], merged_cols: Set[String]) = {
          merged_cols.toList.map(x => x match {
              case x if column.contains(x) => col(x)
              case _ => lit(0).as(x)
            }
          )
        }

        val new_df1 = output_df.select(getNewColumns(output_df.columns.toSet, merged_cols):_*)
        val new_df2 = prev_df.select(getNewColumns(prev_df.columns.toSet, merged_cols):_*)
        output_df = new_df1.unionByName(new_df2)

        /*
        GROUP & SUM
        */
        val groupCol = "uid"
        val aggCols = (output_df.columns.toSet - groupCol)
          .map(colName => sum(colName).alias(colName)).toList
        output_df = output_df.groupBy(groupCol).agg(aggCols.head, aggCols.tail: _*)
      }
    }
    output_df.write.mode("overwrite").parquet(param_output_dir + "/" + max_date)
    println("Data saved: " + param_output_dir + "/" + max_date)

  }

}
