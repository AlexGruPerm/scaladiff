import au.com.bytecode.opencsv.CSVWriter
import bar.{ReadCassandraExamples, rowToX}
import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

case class CsvTick(ts :Long, prc: Double)

case class Tickers(ticker_id :Int, ticker_code :String);

class DataUploader(session: Session) extends rowToX(session, LoggerFactory.getLogger(ReadCassandraExamples.getClass)) {
  val logger = LoggerFactory.getLogger(ReadCassandraExamples.getClass)

  val prepCsvTicks = session.prepare(
    """ select db_tsunx,ask from mts_src.ticks where ticker_id = :tickerId allow filtering """)

  val prepTickers = session.prepare(""" select ticker_id,ticker_code from mts_meta.tickers """)

  val tickersSymbols : Seq[Tickers] = JavaConverters.asScalaIteratorConverter(session.execute(prepTickers.bind()).all().iterator())
                                                    .asScala.toSeq
                                                    .map(r => Tickers(r.getInt("ticker_id"),r.getString("ticker_code"))  )
                                                    .sortBy(_.ticker_id).toList

  def upload(FileName :String,tickerID :Int)={
    logger.info("BEGIN UPLOAD.")

    import java.io.{BufferedWriter, FileWriter}

    val out = new BufferedWriter(new FileWriter("C:\\Users\\Yakushev\\Desktop\\TEST_BC\\"+FileName))
    val writer = new CSVWriter(out,';')

    val employeeSchema = Array("ts","rn", "prc")
    val bondPrepCsvTicks =  prepCsvTicks.bind().setInt("tickerId", tickerID)
    val dsTickersWidths =  JavaConverters.asScalaIteratorConverter(session.execute(bondPrepCsvTicks).all().iterator())
                                         .asScala.toSeq
                                         .map(r => CsvTick(r.getLong("db_tsunx"),r.getDouble("ask"))  )
                                         .sortBy(_.ts).toList
    val listOfRecords = new java.util.ArrayList[Array[String]]()
    listOfRecords.add(employeeSchema)

    for ((rec,idx) <- dsTickersWidths.zipWithIndex){
      listOfRecords.add(Array(rec.ts.toString, (idx+1).toString, rec.prc.toString.replace('.',',')))
    }

    writer.writeAll(listOfRecords)
    writer.flush()
    writer.close()

    logger.info("END UPLOAD.")
  }

}

object CsvUploader extends App {
  val du = new DataUploader(new bar.calculator.SimpleClient("127.0.0.1").session)
  du.tickersSymbols.map(tsmb => du.upload(tsmb.ticker_code+".csv",tsmb.ticker_id))
}


