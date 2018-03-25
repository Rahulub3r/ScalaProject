name := "6347_spark02"

version := "0.1"

scalaVersion := "2.11.8"

lazy val downloadFromZip = taskKey[Unit]("Download the college scorecard zip and extract it")
downloadFromZip := {
  if(java.nio.file.Files.notExists(new File("src/test/resources/CollegeScorecard_Raw_Data").toPath())) {
    println("College scorecard data does not exist, downloading...")
    IO.unzipURL(new URL("https://ed-public-download.app.cloud.gov/downloads/CollegeScorecard_Raw_Data.zip"), new File("src/test/resources"))
    val crosswalkFile = new File("src/test/resources/CollegeScorecard_Raw_Data/Crosswalks.zip")
    IO.delete(crosswalkFile)
  }
}
(Test / test) := ((Test / test).dependsOn(downloadFromZip)).value
(Test / testQuick) := ((Test / testQuick).dependsOn(downloadFromZip)).evaluated

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"
libraryDependencies += "com.github.zuinnote" %% "spark-hadoopoffice-ds" % "1.0.4"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"