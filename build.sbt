name := "Triv.me"

version := "0.0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.2.1",
	"org.apache.spark" %% "spark-mllib" % "1.2.1",
	"org.scalanlp" %% "breeze" % "0.10",
    // native libraries are not included by default. add this if you want them (as of 0.7)
    // native libraries greatly improve performance, but increase jar sizes.
    "org.scalanlp" %% "breeze-natives" % "0.10",
    "org.scalaj" %% "scalaj-http" % "1.1.4",
    "com.typesafe.play" %% "play-json" % "2.3.1",
    "org.scala-lang.modules" %% "scala-async" % "0.9.2"
)

resolvers ++= Seq(
            // other resolvers here
            // if you want to use snapshot builds (currently 0.11-SNAPSHOT), use this.
            "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
            "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
            "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
)