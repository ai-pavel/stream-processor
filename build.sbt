val scala3Version = "3.3.4"

lazy val root = project
  .in(file("."))
  .settings(
    name := "stream-processor",
    version := "0.1.0",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % "0.14.10",
      "io.circe" %% "circe-generic" % "0.14.10",
      "io.circe" %% "circe-parser" % "0.14.10",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    )
  )
