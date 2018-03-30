import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * The data is described here - https://collegescorecard.ed.gov/data/documentation/
  *
  */
object CollegeScorecard {

  /**
    * Load scorecard data using standard csv mechanism.
    *
    * Get column names from the header - option header is true
    * Infers schema from data in the CSV - option nullValue is "NULL"
    * Converts "NULL" as a null value - option inferSchema is true
    *
    * The returned dataframe should have the following columns (in this order):
    * - UNITID - Integer
    * - OPEID - Integer
    * - INSTNM - String
    * - CITY - String
    * - STABBR - String
    * - COSTT4_A - Integer
    * - DEBT_MDN - Double
    * - C100_4 - Double
    * - C150_4 - Double
    *
    * @param spark the spark session
    * @param path the path to the csv file(s)
    * @return a dataframe
    */
  def loadScorecardData(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read
      .format("csv").option("header","true")
      .option("nullValue","NULL")
      .option("inferSchema","true")
      .load(path)
    val subDF = df.select(
      "UNITID", "OPEID","INSTNM","CITY","STABBR","COSTT4_A","DEBT_MDN","C100_4","C150_4"
    )
    val subDF1 = subDF.select(subDF("UNITID").cast(IntegerType),
      subDF("OPEID").cast(IntegerType),
      subDF("INSTNM").cast(StringType),
      subDF("CITY").cast(StringType),
      subDF("STABBR").cast(StringType),
      subDF("COSTT4_A").cast(IntegerType),
      subDF("DEBT_MDN").cast(DoubleType),
      subDF("C100_4").cast(DoubleType),
      subDF("C150_4").cast(DoubleType)
    )
    subDF1
  }

  /**
    * Load scorecard crosswalk data using office plugin
    *
    * @param spark the spark session
    * @param path the path to the excel files(s)
    * @return a dataframe
    */
  def loadCrosswalkData(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("org.zuinnote.spark.office.excel")
      .option("read.lowFootprint", value = true)
      .option("read.sheets", "Crosswalk")
      .option("read.spark.simpleMode",value = true)
      .option("read.spark.useHeader", value = true)
      .option("sheetName", "crosswalk")
      .load(path)
  }

  /**
    * Return a dataframe with five states with highest per academic year cost (COSTT4_A)
    * with the following columns (in this order):
    * - STABBR (state abbreviation) - String
    * - COSTT4_A_MEAN (mean academic year cost) - Double
    *
    * The resulting dataframe must be sorted in descending order of academic year cost
    *
    * @param df the dataframe
    * @return the dataframe
    */
  def fiveMostExpensiveStates(df: DataFrame): DataFrame = {
    val subDf = df.select("STABBR","COSTT4_A").
      where(df("STABBR").isNotNull).
      where(df("COSTT4_A").isNotNull).
      groupBy("STABBR").
      agg(avg("COSTT4_A").alias("COSTT4_A_MEAN")).
      sort(desc("COSTT4_A_MEAN")).limit(5)
    subDf
  }

  /**
    * Return a dataframe with five institutions in Texas with highest median student debt (DEBT_MDN)
    * with the following columns (in this order):
    * - UNITID - Integer
    * - OPEID - Integer
    * - INSTNM (institution name) - String
    * - STABBR (state abbreviation) - String
    * - DEBT_MDN (mean academic year cost) - Double
    *
    * The resulting dataframe must be sorted in descending order of median student debt
    *
    * N.B. You'll have to remove any data where median debt is null
    *
    * @param df the dataframe
    * @return the dataframe
    */
  def fiveTexasCollegesWithHighestDebt(df: DataFrame): DataFrame = {
    val subDf = df.filter(df("STABBR") === "TX").
      select("UNITID", "OPEID", "INSTNM", "STABBR", "DEBT_MDN").
      where(df("DEBT_MDN").isNotNull).
      sort(desc("DEBT_MDN")).
      limit(5)
    subDf
  }

  /**
    * Return a dataframe with the average and sample standard deviation of the expected time completion rate (C100_4)
    * by city in Texas with the following columns (in this order):
    * - CITY - String
    * - C100_4_MEAN - Double
    * - C100_4_STDDEV - Double
    * - COUNT - Long
    *
    * The resulting dataframe must be sorted in descending order of mean expected time completion rate
    *
    * N.B. You'll have to exclude any record where C100_4 is null and
    * any groupings that have a count of 1 (standard deviation is not defined when the count is
    * less than 2)
    *
    * @param df the dataframe
    * @return the dataframe
    */
  def completionRateStatsInTexasByCity(df: DataFrame): DataFrame = {
    val subDf = df.filter(df("STABBR")==="TX").
      select("C100_4","CITY").
      where(df("C100_4").isNotNull).
      groupBy("CITY").
      agg(avg("C100_4").alias("C100_4_MEAN"),
        stddev("C100_4").alias("C100_4_STDDEV"),
        count("C100_4").alias("COUNT")).
      sort(desc("C100_4_MEAN"))
    val subDf1 = subDf.filter(subDf("COUNT")>1)
    subDf1
  }
}
