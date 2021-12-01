package pkg.domain

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}

import java.sql.Date

object Pollution extends Enumeration {

  val DELIMITER = ","

  val FORMAT = "com.databricks.spark.csv"

  val SENSOR_ID,
  TIME_INSTANT,
  MEASUREMENT = Value

  val SCHEMA = new StructType()
    .add(SENSOR_ID.toString, IntegerType, false)
    .add(TIME_INSTANT.toString, StringType, true)
    .add(MEASUREMENT.toString, DoubleType, true)

  case class POLLUTION_MI(SENSOR_ID: Int, TIME_INSTANT: String, MEASUREMENT: Double)

}


