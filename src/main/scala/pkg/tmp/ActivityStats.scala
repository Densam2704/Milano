package pkg.tmp

import org.apache.spark.sql.types.{DoubleType, StructType}

object ActivityStats extends Enumeration {
  val MIN, MAX, AVG = Value

  val DELIMITER = ","

  val SCHEMA = new StructType()
    .add(MIN.toString, DoubleType)
    .add(MAX.toString, DoubleType)
    .add(AVG.toString, DoubleType)
}

case class ActivityStats(MIN: Double, MAX: Double, AVG: Double)
