
name := "newday"

version := "1.0"

scalaVersion := "2.10.6"

resolvers += "apache" at "https://repository.apache.org/content/repositories/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3"

mainClass in assembly := Some("eu.danielsmith.newday.Movielens")

assemblyMergeStrategy in assembly := {
  case PathList("com", "esotericsoftware", xs @ _*)                   => MergeStrategy.first
  case PathList("org", "apache", xs @ _*)                   => MergeStrategy.first
  case PathList("com", "google", xs @ _*)                   => MergeStrategy.first
  case PathList("com", "sun", "research", "ws", xs @ _*)                   => MergeStrategy.first
  case PathList(xs @ _*) if xs.last == "UnusedStubClass.class" => MergeStrategy.first
  case "UnusedStubClass.class"                                 => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}