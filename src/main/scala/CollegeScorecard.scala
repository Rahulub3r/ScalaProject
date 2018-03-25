import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

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
  def loadScorecardData(spark: SparkSession, path: String): DataFrame = ??? // Implement

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
      .option("read.lowFootprint", true)
      .option("read.sheets", "Crosswalk")
      .option("read.spark.simpleMode",true)
      .option("read.spark.useHeader", true)
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
  def fiveMostExpensiveStates(df: DataFrame): DataFrame = ??? //Implement

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
  def fiveTexasCollegesWithHighestDebt(df: DataFrame): DataFrame = ??? //Implement

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
  def completionRateStatsInTexasByCity(df: DataFrame): DataFrame = ??? //Implement

}
