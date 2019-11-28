name := "clark-insights-app"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.hive" % "hive-common" % "2.3.2"
)
libraryDependencies += "com.databricks" % "dbutils-api_2.11" % "0.0.3"


scalaSource in Compile := baseDirectory.value / "src"