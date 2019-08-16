import java.sql.{DriverManager, ResultSet, SQLException}

import scala.util.Random
import scala.collection.mutable.ListBuffer

object App {
  def main(args: Array[String]): Unit = {
    val tableName = args(0)
    val recordsSize = args(1).toInt

    val url = "jdbc:db2://dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:50000/BLUDB"
    val username = "rhl98436"
    val pass = "svjmvdbc0zcg-xz5"

    println("Connection config:" +
      "\njdbcurl: " + url +
      "\nusername: " + username +
      "\npassword: " + pass)

    //create table with @tableName
    val query02 = s"create table ${tableName} " +
      s"(product_id integer, " + s"product_group varchar(10), " + s"year varchar(10), " +
      s"jan_sales integer, " + s"feb_sales integer, " + s"mar_sales integer, " +
      s"apr_sales integer, " + s"may_sales integer, " + s"jun_sales integer, " +
      s"jul_sales integer, " + s"aug_sales integer, " + s"sep_sales integer, " +
      s"oct_sales integer, " + s"nov_sales integer, " + s"dev_sales integer)"

    val query03 = s"insert into ${tableName} " +
      s"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    Class.forName("com.ibm.db2.jcc.DB2Driver")
    val connection = DriverManager.getConnection(url, username, pass)
    println("Connection established")

    try {
      val stmt_createTable = connection.createStatement()
      stmt_createTable.executeUpdate(query02)
      stmt_createTable.close()

      val stmt_updateTable = connection.prepareStatement(query03)
      val records = genRecords(recordsSize)
      println("Records generated")

      records.foreach(r => {
        stmt_updateTable.setString(1, r.product_id)
        stmt_updateTable.setString(2, r.product_group)
        stmt_updateTable.setString(3, r.year)

        stmt_updateTable.setString(4, r.jan_sales.toString)
        stmt_updateTable.setString(5, r.feb_sales.toString)
        stmt_updateTable.setString(6, r.mar_sales.toString)

        stmt_updateTable.setString(7, r.apr_sales.toString)
        stmt_updateTable.setString(8, r.may_sales.toString)
        stmt_updateTable.setString(9, r.jun_sales.toString)

        stmt_updateTable.setString(10, r.jul_sales.toString)
        stmt_updateTable.setString(11, r.aug_sales.toString)
        stmt_updateTable.setString(12, r.sep_sales.toString)

        stmt_updateTable.setString(13, r.oct_sales.toString)
        stmt_updateTable.setString(14, r.nov_sales.toString)
        stmt_updateTable.setString(15, r.dec_sales.toString)

        stmt_updateTable.addBatch()
      })

      stmt_updateTable.executeBatch()
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

  def genRecords(size: Integer): List[productRecord] = {
    val records = ListBuffer.empty[productRecord]
    var remain = size
    var idCounter = 0

    val r1 = Random

    while (remain > 0) {
      val prod_id = idCounter.toString
      val prod_group = r1.nextInt(10).toString
      val prod = product(prod_id, prod_group)

      var record_number = r1.nextInt(5)
      if (record_number > remain) record_number = remain

      val years = genYears(record_number)
      for(a <- 0 until record_number){
        val record = productRecord(prod.product_id,
          prod.product_group,
          years(a).toString,
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

  def genYears(number: Integer): List[Integer] = {
    val res = ListBuffer.empty[Integer]
    val rand = Random

    var counter = number
    while (counter > 0) {
      val year = 2015 + rand.nextInt(4)
      if (!res.contains(year)){
        res.insert(0, year)
        counter -= 1
      }
    }

    res.toList
  }

  case class product(product_id: String, product_group: String)
  case class productRecord(product_id: String,
                            product_group: String,
                            year: String,
                            jan_sales: Integer, feb_sales: Integer, mar_sales: Integer,
                            apr_sales: Integer, may_sales: Integer, jun_sales: Integer,
                            jul_sales: Integer, aug_sales: Integer, sep_sales: Integer,
                            oct_sales: Integer, nov_sales: Integer, dec_sales: Integer)
}
