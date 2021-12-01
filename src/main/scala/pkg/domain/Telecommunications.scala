package pkg.domain

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructType}

object Telecommunications extends Enumeration {
  val DELIMITER = "\t"

  val FORMAT = "com.databricks.spark.csv"

  val SQUARE_ID,
  TIME_INTERVAL,
  COUNTRY_CODE,
  SMS_IN_ACTIVITY,
  SMS_OUT_ACTIVITY,
  CALL_IN_ACTIVITY,
  CALL_OUT_ACTIVITY,
  INTERNET_TRAFFIC_ACTIVITY = Value

  val SCHEMA = new StructType()
    .add(SQUARE_ID.toString, IntegerType, false)
    .add(TIME_INTERVAL.toString, LongType, false)
    .add(COUNTRY_CODE.toString, IntegerType, false)
    .add(SMS_IN_ACTIVITY.toString, DoubleType, true)
    .add(SMS_OUT_ACTIVITY.toString, DoubleType, true)
    .add(CALL_IN_ACTIVITY.toString, DoubleType, true)
    .add(CALL_OUT_ACTIVITY.toString, DoubleType, true)
    .add(INTERNET_TRAFFIC_ACTIVITY.toString, DoubleType, true)

//  def apply(row: String): TELECOMMUNICATIONS_MI = {
//    val array = row.split(DELIMITER, -1)
//    TELECOMMUNICATIONS_MI(
//      array(SQUARE_ID.id).toInt,
//      array(TIME_INTERVAL.id).toLong,
//      array(COUNTRY_CODE.id).toInt,
//      array(SMS_IN_ACTIVITY.id).toDouble,
//      array(SMS_OUT_ACTIVITY.id).toDouble,
//      array(CALL_IN_ACTIVITY.id).toDouble,
//      array(CALL_OUT_ACTIVITY.id).toDouble,
//      array(INTERNET_TRAFFIC_ACTIVITY.id).toDouble
//    )
//  }

  case class TELECOMMUNICATIONS_MI(SQUARE_ID: Int, TIME_INTERVAL: Long,
                                   COUNTRY_CODE: Int, SMS_IN_ACTIVITY: Double, SMS_OUT_ACTIVITY: Double,
                                   CALL_IN_ACTIVITY: Double, CALL_OUT_ACTIVITY: Double, INTERNET_TRAFFIC_ACTIVITY: Double
                                  )

}



