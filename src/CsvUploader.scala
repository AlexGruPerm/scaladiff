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


  def upload(FileName :String,tickerID :Int)={
    logger.info("BEGIN UPLOAD.")

    import java.io.{BufferedWriter, FileWriter}

    val out = new BufferedWriter(new FileWriter("C:\\Users\\Yakushev\\Desktop\\TEST_BC\\"+FileName))
    val writer = new CSVWriter(out,';')

    val employeeSchema = Array("rn","ts", "prc")
    val bondPrepCsvTicks =  prepCsvTicks.bind().setInt("tickerId", tickerId)
    val dsTickersWidths =  JavaConverters.asScalaIteratorConverter(session.execute(bondPrepCsvTicks).all().iterator())
                                         .asScala.toSeq
                                         .map(r => CsvTick(r.getLong("db_tsunx"),r.getDouble("ask"))  )
                                         .sortBy(_.ts).toList
    val listOfRecords = new java.util.ArrayList[Array[String]]()
    listOfRecords.add(employeeSchema)

    for ((rec,idx) <- dsTickersWidths.zipWithIndex){
      listOfRecords.add(Array((idx+1).toString, rec.ts.toString, rec.prc.toString.replace('.',',')))
    }

    writer.writeAll(listOfRecords)
    writer.flush()
    writer.close()

    logger.info("END UPLOAD.")
  }

}

object CsvUploader extends App {
  val du = new DataUploader(new bar.calculator.SimpleClient("127.0.0.1").session)
  du.upload( "EURUSD.csv",1)
  du.upload( "AUDUSD.csv",2)
  du.upload( "GBPUSD.csv",3)
  du.upload( "NZDUSD.csv",4)
  du.upload( "EURCHF.csv",5)
  du.upload( "USDCAD.csv",6)
  du.upload( "USDCHF.csv",7)
  du.upload( "EURCAD.csv",8)
  du.upload( "GBPAUD.csv",9)
  du.upload( "GBPCAD.csv",10)
  du.upload( "GBPCHF.csv",11)
  du.upload( "EURGBP.csv",12)
  du.upload( "GBPNZD.csv",13)
  du.upload( "NZDCAD.csv",14)
}


