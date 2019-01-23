name := "first"
version := "1.0"
scalaVersion := "2.12.7"
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.2")
libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided"

