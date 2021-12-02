package pkg.stages

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pkg.system.Parameters

object STG_4 {


  def getResults(stg_2_df: DataFrame, stg_3_df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    println("Stage 4 started")
    val joined = join_stg_results(stg_2_df, stg_3_df)
    val stg_4_results = groupBySquare(joined)
    val top5CleanZones = getTop5CleanZones(stg_4_results)
    val top5PollutedZones = getTop5PollutedZones(stg_4_results)
    val unknownZones = getUnknownZones(stg_4_results)

    Parameters.writeDFToFile(unknownZones, Parameters.STG_4_unknown_result_path)
    Parameters.writeDFToFile(top5CleanZones, Parameters.STG_4_top5_clean_result_path)
    Parameters.writeDFToFile(top5PollutedZones, Parameters.STG_4_top5_polluted_result_path)
    Parameters.writeDFToFile(stg_4_results, Parameters.STG_4_result_path)

    println("Stage 4 finished")
    stg_4_results
  }

  def join_stg_results(stg_2_df: DataFrame, stg_3_df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val joined = stg_2_df.alias("s2")
      .join(stg_3_df.alias("s3"),
        //если координаты попадают в квадрат, то соединяем
        //        $"s3.SENSOR_LAT" > $"s2.X3" &&
        //          $"s3.SENSOR_LAT" < $"s2.X1" &&
        //          $"s3.SENSOR_LONG" > $"s2.Y4" &&
        //          $"s3.SENSOR_LONG" < $"s2.Y2"

        ($"s2.X_CENTER" - $"s3.SENSOR_LAT") * ($"s2.X_CENTER" - $"s3.SENSOR_LAT") * $"s2.COEFF_X" * $"s2.COEFF_X"
          +
          ($"s2.Y_CENTER" - $"s3.SENSOR_LONG") * ($"s2.X_CENTER" - $"s3.SENSOR_LONG") * $"s2.COEFF_Y" * $"s2.COEFF_Y"

          < $"s3.SENSOR_RADIUS" * $"s3.SENSOR_RADIUS"
        ,
        "left"
      )
      .select($"s2.SQUARE_ID",
        $"s2.SQUARE_ZONE_TYPE",
        $"s2.GENERAL_SQUARE_ACTIVITY",
        $"s2.MIN_SQUARE_ACTIVITY",
        $"s2.MAX_SQUARE_ACTIVITY",
        $"s2.AVG_SQUARE_ACTIVITY",
        $"s2.CELL_ID",
        $"s2.X1",
        $"s2.Y1",
        $"s2.X2",
        $"s2.Y2",
        $"s2.X3",
        $"s2.Y3",
        $"s2.X4",
        $"s2.Y4",
        $"s2.X5",
        $"s2.Y5",
        $"s3.SENSOR_ID",
        $"s3.TIME_INSTANT",
        $"s3.MEASUREMENT",
        $"s3.SENSOR_STREET_NAME",
        $"s3.SENSOR_LAT",
        $"s3.SENSOR_LONG",
        $"s3.SENSOR_RADIUS",
        $"s3.SENSOR_TYPE",
        $"s3.UOM",
        $"s3.TIME_INSTANT_FORMAT"
      )
    joined
  }

  def groupBySquare(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.groupBy($"SQUARE_ID")
      .agg(
        countDistinct($"SENSOR_ID").alias("SENSOR_COUNT"),
        sum($"MEASUREMENT").alias("SQUARE_POLLUTION"),
        min($"MEASUREMENT").alias("MIN_SQUARE_POLLUTION"),
        max($"MEASUREMENT").alias("MAX_SQUARE_POLLUTION"),
        avg($"MEASUREMENT").alias("AVG_SQUARE_POLLUTION"),
        first($"GENERAL_SQUARE_ACTIVITY").alias("SQUARE_ACTIVITY"),
        first($"MIN_SQUARE_ACTIVITY").alias("MIN_SQUARE_ACTIVITY"),
        first($"MAX_SQUARE_ACTIVITY").alias("MAX_SQUARE_ACTIVITY"),
        first($"AVG_SQUARE_ACTIVITY").alias("AVG_SQUARE_ACTIVITY"),
        first($"SQUARE_ZONE_TYPE").alias("SQUARE_ZONE_TYPE")
      )
      .select($"SQUARE_ID",
        $"SENSOR_COUNT",
        $"SQUARE_POLLUTION",
        $"MIN_SQUARE_POLLUTION",
        $"MAX_SQUARE_POLLUTION",
        $"AVG_SQUARE_POLLUTION",
        $"SQUARE_ACTIVITY",
        $"MIN_SQUARE_ACTIVITY",
        $"MAX_SQUARE_ACTIVITY",
        $"AVG_SQUARE_ACTIVITY",
        $"SQUARE_ZONE_TYPE"
      )
      .persist()
  }

  def getTop5PollutedZones(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val res = df
      .where($"SQUARE_ZONE_TYPE" === "living" || $"SQUARE_ZONE_TYPE" === "working")
      .orderBy($"SQUARE_POLLUTION".desc)
      .limit(5)
    res.show()


    res
  }

  def getTop5CleanZones(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val res =
      df
        .where($"SQUARE_ZONE_TYPE" === "living" || $"SQUARE_ZONE_TYPE" === "working")
        .where($"SQUARE_POLLUTION" > 0)
        .orderBy($"SQUARE_POLLUTION".asc)
        .limit(5)
    res.show()
    res

  }


  def getUnknownZones(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val res = df.where($"SENSOR_COUNT" === 0 || $"SQUARE_ZONE_TYPE" === "none")

    res.show

    res

  }

}
