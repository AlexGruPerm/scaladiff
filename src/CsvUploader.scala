import org.slf4j.LoggerFactory
import java.io.StringWriter
import bar.rowToX
import bar.{ReadCassandraExamples, rowToX}
import au.com.bytecode.opencsv.CSVWriter

import scala.collection.JavaConverters._

import java.io.FileWriter
import java.io.BufferedWriter

import java.util.List

import bar.{ReadCassandraExamples, rowToX}
import com.datastax.driver.core.{Row, Session}

class DataUploader(session: Session) extends rowToX(session, LoggerFactory.getLogger(ReadCassandraExamples.getClass)) {
  val logger = LoggerFactory.getLogger(ReadCassandraExamples.getClass)

  val prepared = session.prepare(
    """ select max(ts)       as ts,
                                            max(db_tsunx) as tsunx
                                       from mts_src.ticks
                                      where ticker_id = :tickerId and
                                            ddate     = :maxDDate
                                   """)

  def upload()={
    logger.info("BEGIN UPLOAD.")

    import java.io.BufferedWriter
    import java.io.FileWriter

    val out = new BufferedWriter(new FileWriter("C:\\Users\\Yakushev\\Desktop\\TEST_BC\\ticks.csv"))
    val writer = new CSVWriter(out);
    val employeeSchema = Array("ts", "prc")

    val employee1 = Array("123", "23")
    val employee2 = Array("125", "24")

    //val listOfRecords = List(employeeSchema, employee1, employee2).asJava

    val listOfRecords = new java.util.ArrayList[String]()
    javaList.add(employeeSchema)
    javaList.add(employee1)
    javaList.add(employee2)

    writer.writeAll(listOfRecords)

    logger.info("END UPLOAD.")
  }

}

object CsvUploader extends App {
  val du = new DataUploader(new bar.calculator.SimpleClient("127.0.0.1").session)
  du.upload()
}


