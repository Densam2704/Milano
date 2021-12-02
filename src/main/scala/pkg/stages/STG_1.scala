package pkg.stages

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pkg.system.Parameters
import pkg.tmp.ActivityStats

import java.text.SimpleDateFormat
import scala.util.Try

object STG_1 {

  val living_hours_lower = "00:00:00"
  val working_hours_lower = "09:00:00"
  val working_hours_upper = "17:00:00"
  val working_filename = "/working"
  val living_filename = "/living"

  def getResults(telecommunications_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    println("Stage 1 started")

    val (working_hours_df, living_hours_df) = splitByWorkingHours(telecommunications_df)

    val working = findAndWriteStats(working_hours_df, working_filename)

    val living = findAndWriteStats(living_hours_df, living_filename)

    val united = uniteDFs(working, living)

    val stg_1_result = groupBySquare(united)

//    Parameters.writeDFToFile(stg_1_result, Parameters.STG_1_result_path)

    println("Stage 1 finished")

    stg_1_result
  }


  private def splitByWorkingHours(telecommunications_df: DataFrame)(implicit spark: SparkSession): (DataFrame, DataFrame) = {

    import spark.implicits._

    //извлекает из unix time, часы минуты и секунды
    val get_date_udf = udf((col: String) => {
      val epochTime = Try(col.toLong).getOrElse(0)
      val df: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
      df.format(epochTime)
    })

    val working_hours_df = telecommunications_df
      .withColumn("time", get_date_udf($"time_interval"))
      .where($"time".between(working_hours_lower, working_hours_upper)
      )

    val living_hours_df = telecommunications_df
      .withColumn("time", get_date_udf($"time_interval"))
      .where($"time".between(living_hours_lower, working_hours_lower)
        .or($"time".between(working_hours_upper, living_hours_lower))
      )

    (working_hours_df, living_hours_df)
  }


  private def findAndWriteStats(df: DataFrame, filename: String)(implicit sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._

    val sum_df = df.na
      .fill(0.0)
      .withColumn("SUM_ACTIVITIES",
        $"SMS_IN_ACTIVITY" +
          $"SMS_OUT_ACTIVITY" +
          $"CALL_IN_ACTIVITY" +
          $"CALL_OUT_ACTIVITY" +
          $"INTERNET_TRAFFIC_ACTIVITY"
      )

    val stats = sum_df
      .select($"SQUARE_ID", $"SUM_ACTIVITIES")
      .groupBy($"SQUARE_ID")
      .agg(
        sum($"SUM_ACTIVITIES").alias("SQUARE_SUM")
      )
      .agg(
        min("SQUARE_SUM").alias("MIN"),
        max("SQUARE_SUM").alias("MAX"),
        avg("SQUARE_SUM").alias("AVG")
      )
    Parameters.writeDFToFile(stats, Parameters.STG_1_stats_path + filename)

    val MIN = stats.as[ActivityStats].first.MIN
    val MAX = stats.as[ActivityStats].first.MAX
    val AVG = stats.as[ActivityStats].first.AVG

    //udf. Определяет зону (жилая/рабочая/неопределено)
    val get_zones_udf = udf((sum_activities: String) => {
      val sum = Try(sum_activities.toDouble).getOrElse(0)
      sum match {
        case x: Double if x >= MIN && x < AVG => "living"
        case x: Double if x >= AVG && x <= MAX => "working"
        case _ => "none"
      }
    })

    sum_df.withColumn("ZONE", get_zones_udf($"SUM_ACTIVITIES"))

  }

  private def uniteDFs(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.union(df2).toDF()
  }

  private def groupBySquare(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    df.withColumn("IS_LIVING", when($"ZONE" === "living", 1).otherwise(0))
      .withColumn("IS_WORKING", when($"ZONE" === "working", 1).otherwise(0))
      .groupBy($"SQUARE_ID")
      .agg(
        min("SUM_ACTIVITIES").alias("MIN_SQUARE_ACTIVITY"),
        max("SUM_ACTIVITIES").alias("MAX_SQUARE_ACTIVITY"),
        avg("SUM_ACTIVITIES").alias("AVG_SQUARE_ACTIVITY"),
        sum($"SUM_ACTIVITIES").alias("GENERAL_SQUARE_ACTIVITY"),
        sum($"IS_WORKING").alias("WORKING_CNT"),
        sum($"IS_LIVING").alias("LIVING_CNT")
      )
      .withColumn("SQUARE_ZONE_TYPE", when($"LIVING_CNT" > $"WORKING_CNT", "living")
        .when($"LIVING_CNT" <= $"WORKING_CNT" && $"WORKING_CNT" != 0, "working")
        .otherwise("none")
      )
      .select($"SQUARE_ID",
        $"SQUARE_ZONE_TYPE",
        $"GENERAL_SQUARE_ACTIVITY",
        $"MIN_SQUARE_ACTIVITY",
        $"MAX_SQUARE_ACTIVITY",
        $"AVG_SQUARE_ACTIVITY"
      )

  }


}
