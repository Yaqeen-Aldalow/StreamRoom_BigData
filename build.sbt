ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "StreamRoom",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-streaming" % "3.5.1",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
      "org.apache.kafka" % "kafka-clients" % "3.6.0",
      "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
       "com.google.guava" % "guava" % "33.0.0-jre",
       "com.google.guava" % "guava" % "32.1.3-jre"
    )
  )
