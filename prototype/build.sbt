name := "prototype"

version := "1.0"

scalaVersion := "2.10.0"

org.scalastyle.sbt.ScalastylePlugin.Settings

resolvers ++= Seq(
  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
)

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "com.twitter" %% "algebird-core" % "0.1.13",
  "com.twitter" %% "scalding-core" % "0.8.6"
)
