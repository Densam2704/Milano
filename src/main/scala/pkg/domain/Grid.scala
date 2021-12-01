package pkg.domain

import org.apache.spark.sql.types.{DoubleType, StructType}

object Grid extends Enumeration {

  val FEATURE_COLLECTION,
  FEATURES,
  GEOMETRY,
  COORDINATES,
  X1,
  X2,
  X3,
  X4,
  X5,
  Y1,
  Y2,
  Y3,
  Y4,
  Y5,
  PROPERTIES,
  SQUARE_ID = Value

  val FORMAT = "json"

  val COORDINATES_SCHEMA = new StructType()
    .add("",
      new StructType()
        .add("",
          new StructType()
            .add(X1.toString, DoubleType)
            .add(Y1.toString, DoubleType)
        )
        .add("",
          new StructType()
            .add(X2.toString, DoubleType)
            .add(Y2.toString, DoubleType)
        )
        .add("",
          new StructType()
            .add(X3.toString, DoubleType)
            .add(Y3.toString, DoubleType)
        )
        .add("",
          new StructType()
            .add(X4.toString, DoubleType)
            .add(Y4.toString, DoubleType)
        )
        .add("",
          new StructType()
            .add(X5.toString, DoubleType)
            .add(Y5.toString, DoubleType)
        )
    )

  //  val DELIMITER = ","
  //  val SCHEMA = new StructType()
  //    .add(FEATURE_COLLECTION)

  //  case class MILANO_GRID()

  case class CoordinatesCase(X1:Double,
                             Y1:Double,
                             X2:Double,
                             Y2:Double,
                             X3:Double,
                             Y3:Double,
                             X4:Double,
                             Y4:Double,
                             X5:Double,
                             Y5:Double
                            )
}
