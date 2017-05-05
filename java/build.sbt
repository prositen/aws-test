name := "aws-test-java"

version := "1.0"

scalaVersion := "2.12.2"

resolvers += "Bintray sbt plugin releases" at "http://dl.bintray.com/sbt/sbt-plugin-releases/"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.123"

mainClass in Compile := Some("FreqCap")