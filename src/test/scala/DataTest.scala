import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

class DataTest extends FlatSpec with SparkEnvironment {

  lazy val scoreCardFileDir = "src/test/resources/CollegeScorecard_Raw_Data"

  lazy val scoreCardDF = CollegeScorecard
    .loadScorecardData(spark, scoreCardFileDir + "/*.csv")


  "the college scorecard data" must "be loadable and have the correct schema" in {
    // Make sure we have the correct columns for the College Scorecard data
    val scoreCardSchema = StructType(
      Array(
        StructField("UNITID", IntegerType),
        StructField("OPEID", IntegerType),
        StructField("INSTNM", StringType),
        StructField("CITY", StringType),
        StructField("STABBR", StringType),
        StructField("COSTT4_A", IntegerType),
        StructField("DEBT_MDN", DoubleType),
        StructField("C100_4", DoubleType),
        StructField("C150_4", DoubleType)
      )
    )
    assert(scoreCardDF.count == 139995)
    assert(scoreCardDF.schema == scoreCardSchema)
  }


}
