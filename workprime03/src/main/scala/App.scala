import java.sql.SQLInvalidAuthorizationSpecException

import com.amazonaws.jmespath.InvalidTypeException
import com.amazonaws.services.glue.model.Column
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext, SparkSession, functions}

/**
 * Application Workpime02
 *
 * Get data from db2 table with columns (product_id, product_group, year, [month_sales]).
 * Transform loaded data:
 *   For each row calculate year_purchase by sum [month_sales],
 *   Add year_purchase as a new column,
 *   delete [month_sales] columns.
 * Save new data into Cloud Object Storage bucket.
 */
object App {

  /**
   * Main Application Method
   * @param args - must contain:
   *              (0) - table name in db2 database
   *              (1) - bucket name in cos where to write data
   *              (2) - format for the output file
   *              (3) - output file name (without extension)
   */
  def main(args: Array[String]): Unit = {
    try{
      //Supported file extensions
      val SUPPORTED_FILE_EXTENSIONS = List("txt", "csv", "parquet")

      //init arguments
      val tableName = args(0)
      val bucketName = args(1)
      val format = args(2)
      val output = args(3)

      //check file format
      if (!SUPPORTED_FILE_EXTENSIONS.contains(format))
        throw new InvalidTypeException("Provided file format is unsupported")

      //init spark session
      val ss = SparkSession.builder().getOrCreate()
      val sc: SparkContext = ss.sparkContext
      val sqlContext: SQLContext = ss.sqlContext

      sc.setLogLevel("ERROR")

      //init db2 variables
      val username = sc.getConf.get("spark.db2_username")
      val password = sc.getConf.get("spark.db2_password")
      val jdbc_url = sc.getConf.get("spark.db2_jdbc_url")
      val partitionNum = sc.getConf.get("spark.db2_partition_num").toInt
      val table = tableName

      //init cos variables
      val serviceName = sc.getConf.get("spark.cos_service_name")
      val endpoint = sc.getConf.get("spark.cos_endpoint")
      val accessKey = sc.getConf.get("spark.cos_access_key")
      val secretKey = sc.getConf.get("spark.cos_secret_key")

      //Hadoop FS configuration
      configureHadoopFS(sc, endpoint, accessKey, secretKey, serviceName)

      //get data from db2 table
      val df01 = getDataFromDB2(ss, username, password,
        jdbc_url, table, partitionNum)

      //transform data into new data frame
      val df02 = transformData(ss, df01)

      //write transformed data into cos
      val uri = s"cos://$bucketName.$serviceName/$output.$format"
      df02.write
        .format(format)
        .option("header", "true")
        .save(uri)

      ss.stop()

    } catch {
      case typeEx: InvalidTypeException => {
        println("Invalid output file extension: " + typeEx.getMessage)
        typeEx.printStackTrace()
      }
      case sqlEx: SQLInvalidAuthorizationSpecException => {
        println("Invalid db2 credentials: " + sqlEx.getMessage)
        sqlEx.printStackTrace()
      }
      case argsEx: ArrayIndexOutOfBoundsException => {
        println("Not enough arguments specified: " + argsEx.getMessage)
        argsEx.printStackTrace()
      }
      case sparkConfEx: NoSuchElementException => {
        println("Following key is not specified:" + sparkConfEx.getMessage)
        sparkConfEx.printStackTrace()
      }
      case ex: Exception => {
        println("Unknown exception: " + ex.getMessage)
        ex.printStackTrace()
      }
    }
  }

  /**
   * Load data from db2 table. All params provided by spark parameters.
   * @param spark
   * @param username
   * @param password
   * @param jdbc_url
   * @param table
   * @param partitionsNum
   * @return
   */
  def getDataFromDB2(spark: SparkSession, username: String, password: String,
                     jdbc_url: String, table: String, partitionsNum: Int): DataFrame = {
    val df = spark.read
      .format("jdbc")
      .option("url", jdbc_url)
      .option("driver", "com.ibm.db2.jcc.DB2Driver")
      .option("user", username)
      .option("password", password)
      .option("dbtable", table)
      .option("numPartitions", partitionsNum)
      .load()
      .repartition(partitionExprs = functions.col("YEAR"))

    df
  }

  /**
   * Hadoop file system configuration to use "cos://(bucket).(service)/(object)s" schema.
   * All params provided by environmental variables.
   * @param sc
   * @param endpoint
   * @param accessKey
   * @param secretKey
   */
  def configureHadoopFS(sc: SparkContext, endpoint: String, accessKey: String,
                        secretKey: String, service: String): Unit = {
    sc.hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
    sc.hadoopConfiguration.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    sc.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    sc.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")

    sc.hadoopConfiguration.set(s"fs.cos.$service.endpoint", endpoint)
    sc.hadoopConfiguration.set(s"fs.cos.$service.access.key", accessKey)
    sc.hadoopConfiguration.set(s"fs.cos.$service.secret.key", secretKey)
  }

  /**
   * Transform data with columns (product_id, product_group, year, [month_sales]) by calculating new column
   * year_purchase and deleting [month_sales] for each row.
   * @param spark
   * @param df
   * @return
   */
  def transformData(spark: SparkSession, df: DataFrame): DataFrame = {
    //Create new data schema
    val df02_schema = StructType(
      df.schema.take(3) :+
        StructField("year_purchases", IntegerType)
    )

    //Create new rdd for new data frame
    val df02_rdd = df.rdd.map(r => {
      val sum = r.getInt(3) + r.getInt(4) + r.getInt(5) +
        r.getInt(6) + r.getInt(7) + r.getInt(8) +
        r.getInt(9) + r.getInt(10) + r.getInt(11) +
        r.getInt(12) + r.getInt(13) + r.getInt(14)

      Row(r(0), r(1), r(2), sum)
    })

    //Create new data frame
    val df02 = spark.createDataFrame(df02_rdd, df02_schema)

    df02
  }
}
