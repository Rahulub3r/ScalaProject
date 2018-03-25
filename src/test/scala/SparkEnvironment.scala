import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkEnvironment extends BeforeAndAfterAll { this: Suite =>
  var spark: SparkSession = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
    spark = null
  }

}
