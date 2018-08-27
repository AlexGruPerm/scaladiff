import au.com.bytecode.opencsv.CSVWriter
import bar.{ReadCassandraExamples, rowToX}
import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

case class CsvTick(ts :Long,prc: Double)

class DataUploader(session: Session) extends rowToX(session, LoggerFactory.getLogger(ReadCassandraExamples.getClass)) {
  val logger = LoggerFactory.getLogger(ReadCassandraExamples.getClass)

  val prepCsvTicks = session.prepare(
    """ select db_tsunx,ask from mts_src.ticks where ticker_id = :tickerId allow filtering """)


  def upload()={
    logger.info("BEGIN UPLOAD.")

    import java.io.{BufferedWriter, FileWriter}

    val out = new BufferedWriter(new FileWriter("C:\\Users\\Yakushev\\Desktop\\TEST_BC\\ticks.csv"))
    val writer = new CSVWriter(out,';')

    /*
    val employeeSchema = Array("ts", "prc")
    val employee1 = Array("123", "23")
    val employee2 = Array("125", "24")
    val listOfRecords = new java.util.ArrayList[Array[String]]()
    listOfRecords.add(employeeSchema)
    listOfRecords.add(employee1)
    listOfRecords.add(employee2)
    writer.writeAll(listOfRecords)
    writer.flush()
    writer.close()
    */

    val employeeSchema = Array("ts", "prc")
    val bondPrepCsvTicks =  prepCsvTicks.bind().setInt("tickerId", 2)
    val dsTickersWidths =  JavaConverters.asScalaIteratorConverter(session.execute(bondPrepCsvTicks).all().iterator())
                                         .asScala.toSeq
                                         .map(r => CsvTick(r.getLong("db_tsunx"),r.getDouble("ask"))  )
                                         .sortBy(_.ts).toList
    val listOfRecords = new java.util.ArrayList[Array[String]]()
    listOfRecords.add(employeeSchema)

    for (rec <- dsTickersWidths){
      listOfRecords.add(Array(rec.ts.toString,rec.prc.toString.replace('.',',')))
    }

    writer.writeAll(listOfRecords)
    writer.flush()
    writer.close()

    logger.info("END UPLOAD.")
  }

}

object CsvUploader extends App {
  val du = new DataUploader(new bar.calculator.SimpleClient("127.0.0.1").session)
  du.upload()
}


