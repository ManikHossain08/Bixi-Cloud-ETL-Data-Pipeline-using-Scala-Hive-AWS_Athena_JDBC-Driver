name := "course8project"

version := "0.1"

scalaVersion := "2.12.12"

val hadoopVersion = "2.7.7"
val jdbcVersion = "1.1.0-cdh5.16.2"
val circeVersion = "0.14.1"

val hadoopDeps = Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
)
val hiveDeps = Seq(
  "org.apache.hive" % "hive-jdbc" % jdbcVersion
)

val amazonS3 = Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.0.002"
)
val athena = Seq(
"software.amazon.awssdk" % "athena" % "2.15.79"
)

val simbaAthena = Seq(
  "com.syncron.amazonaws" % "simba-athena-jdbc-driver" % "2.0.2"
)


libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies ++=hadoopDeps++ hiveDeps++ amazonS3++ athena++ simbaAthena
resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

