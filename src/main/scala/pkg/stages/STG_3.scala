package pkg.stages

import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SparkSession}
import pkg.system.Parameters

object STG_3 {

  def getResults(pollution_df: DataFrame, legend_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    println("Stage 3 started")

    val stg_3_result = joinDFs(pollution_df, legend_df)

//    Parameters.writeDFToFile(stg_3_result, Parameters.STG_3_result_path)

    println("Stage 3 finished")

    stg_3_result
  }

  def joinDFs(pollution_df: DataFrame, legend_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val modified_legend_df = legend_df.where($"SENSOR_TYPE".notEqual("Ozono")
      .and($"SENSOR_TYPE".notEqual("Ozone")))

    val joined = pollution_df.alias("p")
      .join(
        modified_legend_df.alias("l"),
        $"p.SENSOR_ID" === $"l.SENSOR_ID",
        "inner"
      )
      .select($"p.SENSOR_ID".alias("SENSOR_ID"),
        $"TIME_INSTANT",
        $"MEASUREMENT",
        $"SENSOR_STREET_NAME",
        $"SENSOR_LAT",
        $"SENSOR_LONG",
        $"SENSOR_TYPE",
        $"UOM",
        $"TIME_INSTANT_FORMAT"
      )
      //преобразуем все измерения к г/м3
      .withColumn("NEW_MEASUREMENT", when($"UOM" === ("mg/m3"), $"MEASUREMENT" / 1000)
        .when($"UOM" === ("g/m"), $"MEASUREMENT" * 100)
        .when($"UOM" === ("ppb"), $"MEASUREMENT" * 0.12 / 1000000000)
      )
      .drop($"MEASUREMENT")
      .withColumnRenamed("NEW_MEASUREMENT", "MEASUREMENT")
      //заполнить радиус
      .withColumn("SENSOR_RADIUS", when($"SENSOR_ID" > 0, 500))
      .select($"SENSOR_ID",
        $"TIME_INSTANT",
        $"MEASUREMENT",
        $"SENSOR_STREET_NAME",
        $"SENSOR_LAT",
        $"SENSOR_LONG",
        $"SENSOR_TYPE",
        $"UOM",
        $"TIME_INSTANT_FORMAT",
        $"SENSOR_RADIUS"
      )

    joined

  }
}
