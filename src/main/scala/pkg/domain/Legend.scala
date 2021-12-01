package pkg.domain


import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object Legend extends Enumeration {

  val DELIMITER = ","

  val FORMAT = "csv"

  val SENSOR_ID,
  SENSOR_STREET_NAME,
  SENSOR_LAT,
  SENSOR_LONG,
  SENSOR_TYPE,
  UOM,
  TIME_INSTANT_FORMAT= Value

  val SCHEMA = new StructType()
    .add(SENSOR_ID.toString,IntegerType,false)
    .add(SENSOR_STREET_NAME.toString,StringType,true)
    .add(SENSOR_LAT.toString,DoubleType,true)
    .add(SENSOR_LONG.toString,DoubleType,true)
    .add(SENSOR_TYPE.toString,StringType,true)
    .add(UOM.toString,StringType,true)
    .add(TIME_INSTANT_FORMAT.toString,StringType,true)

  case class LEGEND (SENSOR_ID:Int,
                     SENSOR_STREET_NAME:String,
                     SENSOR_LAT:Double,
                     SENSOR_LONG:Double,
                     SENSOR_TYPE: String,
                     UOM:String,
                     TIME_INSTANT_FORMAT:String
                    )

}
