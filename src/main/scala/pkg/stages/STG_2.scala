package pkg.stages

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pkg.system.Parameters


object STG_2 {

  def getResults(stg1_df: DataFrame, grid_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    println("Stage 2 started")

    val STG2_result = joinDFs(stg1_df: DataFrame, grid_df: DataFrame)

    Parameters.writeDFToFile(STG2_result, Parameters.STG_2_result_path)

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

    stg_2_result

  }
}
