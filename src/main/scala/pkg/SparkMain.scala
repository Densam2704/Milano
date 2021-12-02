package pkg

import org.apache.spark.sql.SparkSession
import pkg.stages.{STG_1, STG_2, STG_3, STG_4}
import pkg.system.Parameters


object SparkMain {

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Milano")
      .getOrCreate()
    Parameters.initialisePaths(args)

    import spark.implicits._
    println(spark.sparkContext.uiWebUrl.get)

    val telecommunications_df = Parameters.readTelecommunications()

    val pollution_df = Parameters.readPollution()

    val legend_df = Parameters.readLegend()

    val grid_df = Parameters.readGrid()


    //    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //    telecommunications_df.show()
    //    legend_df.show()
    //    pollution_df.show()
    //    grid_df.show()

    //STG_1
    val stg_1_results = STG_1.getResults(telecommunications_df)(spark)
    //STG_2
    val stg_2_results = STG_2.getResults(stg_1_results, grid_df)(spark)
    //STG_3
    val stg_3_results = STG_3.getResults(pollution_df, legend_df)(spark)
    //STG_4
    val stg_4_results = STG_4.getResults(stg_2_results, stg_3_results)(spark)

    //    val test = Parameters.readDF(name = "stg_2_result", path = Parameters.STG_2_result_path, delimiter = ",")


    //    val clean = Parameters.readDF(name = Parameters.STG_4_top5_clean_result_name, path = Parameters.STG_4_top5_clean_result_path, delimiter = ",")
    //      .select(
    //        $"_c0".alias("SQUARE_ID"),
    //        $"_c1".alias("SENSOR_COUNT"),
    //        $"_c10".alias("SQUARE_ZONE_TYPE"),
    //        $"_c2".alias("SQUARE_POLLUTION"),
    //        $"_c3".alias("MIN_SQUARE_POLLUTION"),
    //        $"_c4".alias("MAX_SQUARE_POLLUTION"),
    //        $"_c5".alias("AVG_SQUARE_POLLUTION"),
    //        $"_c6".alias("SQUARE_ACTIVITY"),
    //        $"_c7".alias("MIN_SQUARE_ACTIVITY"),
    //        $"_c8".alias("MAX_SQUARE_ACTIVITY"),
    //        $"_c9".alias("AVG_SQUARE_ACTIVITY")
    //
    //      )
    //    val polluted = Parameters.readDF(name = Parameters.STG_4_top5_polluted_result_name, path = Parameters.STG_4_top5_polluted_result_path, delimiter = ",")
    //      .select(
    //        $"_c0".alias("SQUARE_ID"),
    //        $"_c1".alias("SENSOR_COUNT"),
    //        $"_c10".alias("SQUARE_ZONE_TYPE"),
    //        $"_c2".alias("SQUARE_POLLUTION"),
    //        $"_c3".alias("MIN_SQUARE_POLLUTION"),
    //        $"_c4".alias("MAX_SQUARE_POLLUTION"),
    //        $"_c5".alias("AVG_SQUARE_POLLUTION"),
    //        $"_c6".alias("SQUARE_ACTIVITY"),
    //        $"_c7".alias("MIN_SQUARE_ACTIVITY"),
    //        $"_c8".alias("MAX_SQUARE_ACTIVITY"),
    //        $"_c9".alias("AVG_SQUARE_ACTIVITY")
    //
    //      )
    //    val unknown = Parameters.readDF(name = Parameters.STG_4_unknown_result_name, path = Parameters.STG_4_unknown_result_path, delimiter = ",")
    //      .select(
    //        $"_c0".alias("SQUARE_ID"),
    //        $"_c1".alias("SENSOR_COUNT"),
    //        $"_c10".alias("SQUARE_ZONE_TYPE"),
    //        $"_c2".alias("SQUARE_POLLUTION"),
    //        $"_c3".alias("MIN_SQUARE_POLLUTION"),
    //        $"_c4".alias("MAX_SQUARE_POLLUTION"),
    //        $"_c5".alias("AVG_SQUARE_POLLUTION"),
    //        $"_c6".alias("SQUARE_ACTIVITY"),
    //        $"_c7".alias("MIN_SQUARE_ACTIVITY"),
    //        $"_c8".alias("MAX_SQUARE_ACTIVITY"),
    //        $"_c9".alias("AVG_SQUARE_ACTIVITY")
    //
    //      )
    //
    //    println("Top 5 clean")
    //    clean.show
    //    println("Top 5 polluted")
    //    polluted.show()
    //    println("Unknown")
    //    unknown.show()
    //  }
  }

}
