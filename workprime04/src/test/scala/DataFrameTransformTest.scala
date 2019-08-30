import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSpec
import org.scalamock.scalatest.MockFactory
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

/**
 * DataFrame Test class
 */
class DataFrameTransformTest extends FunSpec with MockFactory
  with DataFrameComparer with SparkSessionWrapper {

  /**
   * Input DataFrame Schema
   */
  val data01_schema = StructType(
    StructField("PRODUCT_ID", IntegerType) ::
      StructField("PRODUCT_GROUP", IntegerType) ::
      StructField("YEAR", StringType) ::
      StructField("JAN_SALES", IntegerType) ::
      StructField("FEB_SALES", IntegerType) ::
      StructField("MAR_SALES", IntegerType) ::
      StructField("APR_SALES", IntegerType) ::
      StructField("MAY_SALES", IntegerType) ::
      StructField("JUN_SALES", IntegerType) ::
      StructField("JUL_SALES", IntegerType) ::
      StructField("AUG_SALES", IntegerType) ::
      StructField("SPT_SALES", IntegerType) ::
      StructField("OCT_SALES", IntegerType) ::
      StructField("NOV_SALES", IntegerType) ::
      StructField("DEC_SALES", IntegerType) :: Nil
  )

  /**
   * Input DataFrame RDD
   */
  val data01_rdd = spark.sparkContext.parallelize(List(
      Row(1, 3, "2015", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
      Row(1, 3, "2016", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
      Row(2, 3, "2017", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
      Row(2, 3, "2018", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
    )
  )

  /**
   * Output DataFrame Schema
   */
  val data02_schema = StructType(
    StructField("PRODUCT_ID", IntegerType) ::
      StructField("PRODUCT_GROUP", IntegerType) ::
      StructField("YEAR", StringType) ::
      StructField("YEAR_PURCHASES", IntegerType) :: Nil
  )

  /**
   * Output DataFrame RDD
   */
  val data02_rdd = spark.sparkContext.parallelize(List(
      Row(1, 3, "2015", 12),
      Row(1, 3, "2016", 12),
      Row(2, 3, "2017", 12),
      Row(2, 3, "2018", 12)
    )
  )

  /**
   * Fake reading from db2 (App.getDataFromDB2)
   */
  val getDataFromDB2_fake = mockFunction[DataFrame]
  getDataFromDB2_fake.expects().returning(spark.createDataFrame(data01_rdd, data01_schema))

  /**
   * Output DataFrame
   */
  val data02_df = spark.createDataFrame(data02_rdd, data02_schema)

  /**
   * DataFrame Comparison Tests
   */
  describe("DataFrame check") {

    /**
     * Comparison of Input and Output DataFrames
     */
    it("DF02_fake should be equal to App.transformData(DF01_fake)") {
      val df01_fake = getDataFromDB2_fake()
      val df02_fake = App.transformData(spark, df01_fake)

      assertLargeDataFrameEquality(data02_df, df02_fake)
    }
  }
}
