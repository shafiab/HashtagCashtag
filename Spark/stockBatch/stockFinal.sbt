//import AssemblyKeys._
//import net.virtualvoid.sbt.graph.Plugin.graphSettings

//assemblySettings

name := "stockFinal"
version := "1.0"
scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.6.0"
//libraryDependencies += "joda-time" % "joda-time" % "2.7"
libraryDependencies += "joda-time" % "joda-time" % "2.7" withSources()
libraryDependencies += "org.joda" % "joda-convert" % "1.2" withSources() 
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha1" withSources()


