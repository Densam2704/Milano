lazy val root = (project in file(".")).
  settings(
    name := "Milano",
    version := "0.1",
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("pkg.SparkMain")
  )

libraryDependencies ++= Seq(
  "net.ruippeixotog" %% "scala-scraper" % "2.2.0",
  "org.twitter4j" % "twitter4j-core" % "3.0.5",
  "org.twitter4j" % "twitter4j-stream" % "3.0.5",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0"
)
