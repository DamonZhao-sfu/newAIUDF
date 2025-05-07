ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "spark-semantic-plugin",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.3.1" % Provided,
      "org.apache.spark" %% "spark-catalyst" % "3.3.1" % Provided,
      "org.apache.arrow" % "arrow-c-data" % "12.0.0"
    ),
    // sbt-assembly settings
    assembly / mainClass := Some("SemFilterExample"),
    assembly / assemblyJarName := "spark-semantic-plugin_2.12-0.1.0-SNAPSHOT.jar",
    // Merge strategy to avoid conflicts
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
