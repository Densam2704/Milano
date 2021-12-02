package pkg.system

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{first, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import pkg.domain.{Grid, Legend, Pollution, Telecommunications}

object Parameters {

  //имена исходных данных
  val telecommunications_name = "telecommunications"
  //  val telecommunications_december_name = "telecommunications_december"
  //  val telecommunications_november_name = "telecommunications_november"
  val pollution_name = "pollution"
  val legend_name = "legend"
  val grid_name = " grid"

  //имена промежуточных и результирующих данных
  val STG_1_stats_name = "stats"

  val STG_1_result_name = "STG_1_result"
  val STG_2_result_name = "STG_2_result"
  val STG_3_result_name = "STG_3_result"
  val STG_4_result_name = "STG_4_result"

  val STG_4_top5_polluted_result_name = "top5_polluted"
  val STG_4_top5_clean_result_name = "top5_clean"
  val STG_4_unknown_result_name = "unknown"

  //пути к исходным данным
  var telecommunications_path = "/data/Milano/telecommunications/*"
  //  var telecommunications_december_path = "/data/Milano/telecommunications/december/*"
  //  var telecommunications_november_path = "/data/Milano/telecommunications/november/*"
  var legend_path = "/data/Milano/pollution-legend-mi.csv"
  var pollution_path = "/data/Milano/pollution-mi/*"
  var grid_path = "/data/Milano/milano-grid/*"
  var output_path = "/output"

  //пути к промежуточным и итоговым данным
  var STG_1_stats_path = output_path + "/STG_1/" + STG_1_stats_name

  var STG_1_result_path = output_path + "/STG_1/" + STG_1_result_name
  var STG_2_result_path = output_path + "/STG_2/" + STG_2_result_name
  var STG_3_result_path = output_path + "/STG_3/" + STG_3_result_name
  var STG_4_result_path = output_path + "/STG_4/" + STG_4_result_name

  var STG_4_top5_polluted_result_path = output_path + "/STG_4/" + STG_4_top5_polluted_result_name
  var STG_4_top5_clean_result_path = output_path + "/STG_4/" + STG_4_top5_clean_result_name
  var STG_4_unknown_result_path = output_path + "/STG_4/" + STG_4_unknown_result_name

  def initialisePaths(args: Array[String]): Unit = {
    args.length match {
      case 1 =>
        telecommunications_path = args(0) + "/Milano/telecommunications/*"
        legend_path = args(0) + "/Milano/pollution-legend-mi.csv"
        pollution_path = args(0) + "/Milano/pollution-mi/*"
        grid_path = args(0) + "/Milano/milano-grid/*"
        output_path = args(0) + "/output"
        updateOutputPaths(output_path)
      case _ => println("Wrong arguments! Using default paths.")
    }
  }

  private def updateOutputPaths(output_path: String): Unit = {
    STG_1_stats_path = output_path + "/STG_1/" + STG_1_stats_name

    STG_1_result_path = output_path + "/STG_1/" + STG_1_result_name
    STG_2_result_path = output_path + "/STG_2/" + STG_2_result_name
    STG_3_result_path = output_path + "/STG_3/" + STG_3_result_name
    STG_4_result_path = output_path + "/STG_4/" + STG_4_result_name

    STG_4_top5_polluted_result_path = output_path + "/STG_4/" + STG_4_top5_polluted_result_name
    STG_4_top5_clean_result_path = output_path + "/STG_4/" + STG_4_top5_clean_result_name
    STG_4_unknown_result_path = output_path + "/STG_4/" + STG_4_unknown_result_name
  }

  def readGrid()(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    flattenDF(readDF(name = grid_name, path = grid_path, format = Grid.FORMAT))
      .select($"features_properties_cellId".alias("CELL_ID"),
        $"features_geometry_coordinates".alias("COORDINATES")
      )
      .withColumn("rn", row_number().over(Window.partitionBy($"CELL_ID").orderBy($"CELL_ID")))
      .groupBy($"CELL_ID")
      .pivot($"rn")
      .agg(first($"COORDINATES"))
      .withColumnRenamed("1", "Y1")
      .withColumnRenamed("2", "X1")
      .withColumnRenamed("3", "Y2")
      .withColumnRenamed("4", "X2")
      .withColumnRenamed("5", "Y3")
      .withColumnRenamed("6", "X3")
      .withColumnRenamed("7", "Y4")
      .withColumnRenamed("8", "X4")
      .withColumnRenamed("9", "Y5")
      .withColumnRenamed("10", "X5")
  }

  //Для json
  def flattenDF(df: DataFrame): DataFrame = {
    //Источник
    //https://gist.github.com/fahadsiddiqui/d5cff15698f9dc57e2dd7d7052c6cc43
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)

    for (i <- fields.indices) {
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      fieldType match {
        case _: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(
            s"explode_outer($fieldName) as $fieldName"
          )
          val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
          return flattenDF(explodedDf)
        case structType: StructType =>
          val childFieldNames =
            structType.fieldNames.map(childname => fieldName + "." + childname)
          val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFieldNames
          import org.apache.spark.sql.functions.col

          val renamedCols =
            newFieldNames.map { x =>
              col(x.toString).as(x.toString.replace(".", "_"))
            }

          val explodedDf = df.select(renamedCols: _*)
          return flattenDF(explodedDf)
        case _ =>
      }
    }

    df
  }

  def readDF(name: String, schema: StructType = null, path: String,
             delimiter: String = "\t", format: String = "com.databricks.spark.csv")
            (implicit spark: SparkSession): DataFrame = {

    (format, schema) match {
      case (frmt: String, _) if frmt.toLowerCase.contains("json") =>
        val df = spark.read
          .option("multiLine", "true")
          .json(path)
        df.createTempView(name)
        df
      case (_, sch: StructType) if sch.ne(null) =>
        val df = spark.read
          .format(format)
          .options(
            Map(
              "delimiter" -> delimiter,
              "nullValue" -> "\\N"
            )
          )
          .schema(schema)
          .load(path)
        df.createTempView(name)
        df

      case _ =>
        val df = spark.read
          .format(format)
          .options(
            Map(
              "delimiter" -> delimiter,
              "nullValue" -> "\\N"
            )
          )
          .load(path)
        df.createTempView(name)
        df


    }
  }

  def readPollution()(implicit sparkSession: SparkSession): DataFrame = {
    readDF(
      pollution_name,
      Pollution.SCHEMA,
      pollution_path,
      Pollution.DELIMITER,
      Pollution.FORMAT
    )
  }

  def readLegend()(implicit sparkSession: SparkSession): DataFrame = {
    readDF(
      legend_name,
      Legend.SCHEMA,
      legend_path,
      Legend.DELIMITER,
      Legend.FORMAT
    )
  }

  def readTelecommunications()(implicit sparkSession: SparkSession): DataFrame = {

    val telecommunications_df = readDF(
      telecommunications_name,
      Telecommunications.SCHEMA,
      telecommunications_path,
      Telecommunications.DELIMITER
    )
    telecommunications_df
  }

  def writeDFToFile(df: DataFrame, fileName: String, format: String = "com.databricks.spark.csv")
                   (implicit spark: SparkSession): Unit = {

    val fs = FileSystem.get(spark.sessionState.newHadoopConf())

    // удалить, если файл существует
    if (fs.exists(new Path(fileName))) {
      println(s"Deleting previous version of ${fileName}")
      fs.delete(new Path(fileName), true)
    }

    df.write
      .format(format)
      .save(fileName)
    println(s"Dataframe was successfully written to ${fileName}")

  }


  //  def initTables(implicit spark: SparkSession): Unit = {
  //    //    createTable(Parameters.table_laptop, Laptop.structType, Parameters.path_laptop)
  //    //    createTable(Parameters.table_pc, PC.structType, Parameters.path_pc)
  //    //    createTable(Parameters.table_printer, Printer.structType, Parameters.path_printer)
  //    //    createTable(Parameters.table_product, Product.structType, Parameters.path_product)
  //  }


}
