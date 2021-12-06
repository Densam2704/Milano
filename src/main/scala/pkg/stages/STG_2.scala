package pkg.stages

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pkg.system.Parameters

import scala.util.Try


object STG_2 {

  private def get_coeff_udf = udf((x1: Double, x2: Double) => {
    val d = 235
    (x1, x2) match {
      case (x1: Double, x2: Double) if x1 < x2 => d / (x2 - x1)
      case (x1: Double, x2: Double) if x2 < x1 => d / (x1 - x2)
      case _ => 0
    }

  })

  def getResults(stg1_df: DataFrame, grid_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    println("Stage 2 started")

    val STG2_result = joinDFs(stg1_df: DataFrame, grid_df: DataFrame)

    //    Parameters.writeDFToFile(STG2_result, Parameters.STG_2_result_path)

    println("Stage 2 finished")

    STG2_result

  }

  private def joinDFs(stg1_df: DataFrame, grid_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val stg_2_result = stg1_df.alias("s")
      .join(
        grid_df.alias("g"),
        col("SQUARE_ID") === col("CELL_ID"),
        "left"
      )
      .select(
        $"SQUARE_ID",
        $"SQUARE_ZONE_TYPE",
        $"GENERAL_SQUARE_ACTIVITY",
        $"MIN_SQUARE_ACTIVITY",
        $"MAX_SQUARE_ACTIVITY",
        $"AVG_SQUARE_ACTIVITY",
        $"CELL_ID",
        $"X1",
        $"Y1",
        $"X2",
        $"Y2",
        $"X3",
        $"Y3",
        $"X4",
        $"Y4",
        $"X5",
        $"Y5"
      )
      //Центр квадрата
      .withColumn("X_Center", ($"X1" + $"X2") / 2)
      .withColumn("Y_Center", ($"Y1" + $"Y3") / 2)
      //Коэффициент перевода расстояния в координатах X в метры
      .withColumn("COEFF_X", get_coeff_udf($"X1", $"X2"))
      //Коэффициент перевода расстояния в координатах Y в метры
      .withColumn("COEFF_Y", get_coeff_udf($"Y1", $"Y3"))

    stg_2_result

  }
}
