import java.sql.{DriverManager, ResultSet, SQLException}

import scala.util.Random
import scala.collection.mutable.ListBuffer

/**
 * Workprime01 applicaton.
 *
 * @author Andrei Parfionov
 */
object App {

  /**
   * Main application method.
   * Arguments that must be passed:
   * args(0) - table name
   * args(1) - records number
   * @param args
   */
  def main(args: Array[String]): Unit = {
    //creating @tableName and @recordSize from cmd arguments
    val tableName = args(0)
    val recordsSize = args(1).toInt

    //initializing @url, @username and @pass from env variables
    val url = sys.env("DB2_JDBC-URL")
    val username = sys.env("DB2_USERNAME")
    val pass = sys.env("DB2_PASSWORD")

    //query to create table with @tableName
    val query02 = s"""create table ${tableName}
      (product_id integer, product_group integer, year varchar(10),
       jan_sales integer, feb_sales integer, mar_sales integer,
      apr_sales integer, may_sales integer, jun_sales integer,
      jul_sales integer, aug_sales integer, sep_sales integer,
      oct_sales integer, nov_sales integer, dev_sales integer)"""

    //query to insert values into the table
    val query03 = s"""insert into ${tableName}
      values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

    //loading db2 driver into the runtime
    Class.forName("com.ibm.db2.jcc.DB2Driver")
    val connection = DriverManager.getConnection(url, username, pass)
    println("Connection established")

    try {
      //Statement to create new table
      val stmt_createTable = connection.createStatement()
      stmt_createTable.executeUpdate(query02)
      stmt_createTable.close()

      //PreparedStatement to insert multiple rows into table
      val stmt_updateTable = connection.prepareStatement(query03)

      //Generating records
      val records = genRecords(recordsSize)
      println("Records generated")

      //Build query to insert records
      records.foreach(r => {
        stmt_updateTable.setInt(1, r.product_id)
        stmt_updateTable.setInt(2, r.product_group)
        stmt_updateTable.setString(3, r.year)

        stmt_updateTable.setInt(4, r.jan_sales)
        stmt_updateTable.setInt(5, r.feb_sales)
        stmt_updateTable.setInt(6, r.mar_sales)

        stmt_updateTable.setInt(7, r.apr_sales)
        stmt_updateTable.setInt(8, r.may_sales)
        stmt_updateTable.setInt(9, r.jun_sales)

        stmt_updateTable.setInt(10, r.jul_sales)
        stmt_updateTable.setInt(11, r.aug_sales)
        stmt_updateTable.setInt(12, r.sep_sales)

        stmt_updateTable.setInt(13, r.oct_sales)
        stmt_updateTable.setInt(14, r.nov_sales)
        stmt_updateTable.setInt(15, r.dec_sales)

        stmt_updateTable.addBatch()
      })

      //Insert records
      stmt_updateTable.executeBatch()
      stmt_updateTable.close()
      println("Records inserted")
    } catch {
      case ex: Exception => {
        println(ex.getMessage)
        println(ex.printStackTrace())
      }
    } finally {
      connection.close()
    }
  }

  /**
   * This method is used to generate specific number of records satisfying following requirements:
   *            1. There are no duplicates for the same year/product
   *            2. For multiple product rows for different years, product/group combination is concise.
   *
   * @param size Number of records to generate
   * @return List[productRecord] List of generated ProductRecord's
   * */
  def genRecords(size: Integer): List[ProductRecord] = {
    val records = ListBuffer.empty[ProductRecord]
    var remain = size
    var idCounter = 0

    val r1 = Random

    //While there is amount of records to generate new Product created with id (+1 from prev.) and random group (1 - 9)
    //Then method generates number of records (from 1 to 4) with the list of years (2015 - 2018)
    //After that method generates records with random month sales and inserts it into the result list
    while (remain > 0) {
      val prod_id = idCounter
      val prod_group = r1.nextInt(10)

      var record_number = r1.nextInt(5)
      if (record_number > remain) record_number = remain

      val years = genYears(record_number)
      for(a <- 0 until record_number){
        val record = ProductRecord(prod_id,
          prod_group,
          years(a),
          r1.nextInt(100000),
          r1.nextInt(100000),
          r1.nextInt(100000),
          r1.nextInt(100000),
          r1.nextInt(100000),
          r1.nextInt(100000),
          r1.nextInt(100000),
          r1.nextInt(100000),
          r1.nextInt(100000),
          r1.nextInt(100000),
          r1.nextInt(100000),
          r1.nextInt(100000)
        )

        records.insert(0, record)
        remain -= 1
      }

      idCounter += 1
    }

    records.toList
  }

  /**
   * This method is used to generate specific number of years from 2015 to 2018 with no repeat:
   * @param size Number of years to generate
   * @return List[productRecord] List of generated years
   * */
  def genYears(size: Integer): List[String] = {
    val res = ListBuffer.empty[String]
    val rand = Random

    var counter = size
    while (counter > 0) {
      val year = (2015 + rand.nextInt(4)).toString
      if (!res.contains(year)){
        res.insert(0, year)
        counter -= 1
      }
    }

    res.toList
  }

  /**
   * ProductRecord definition
   * @param product_id
   * @param product_group
   * @param year
   * @param jan_sales
   * @param feb_sales
   * @param mar_sales
   * @param apr_sales
   * @param may_sales
   * @param jun_sales
   * @param jul_sales
   * @param aug_sales
   * @param sep_sales
   * @param oct_sales
   * @param nov_sales
   * @param dec_sales
   */
  case class ProductRecord(product_id: Integer,
                           product_group: Integer,
                           year: String,
                           jan_sales: Integer, feb_sales: Integer, mar_sales: Integer,
                           apr_sales: Integer, may_sales: Integer, jun_sales: Integer,
                           jul_sales: Integer, aug_sales: Integer, sep_sales: Integer,
                           oct_sales: Integer, nov_sales: Integer, dec_sales: Integer)
}
