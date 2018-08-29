import au.com.bytecode.opencsv.CSVWriter
import bar.{ReadCassandraExamples, rowToX}
import com.datastax.driver.core.Session
import org.sameersingh.scalaplot.Implicits._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

case class CsvTick(ts :Long, prc: Double)

case class CsvBar(ts_begin :Long,ts_end :Long,o :Double,h :Double,l :Double,c :Double,log_co :Double,disp :Double,ticks_cnt :Int)

case class Tickers(ticker_id :Int, ticker_code :String);

class DataUploader(session: Session) extends rowToX(session, LoggerFactory.getLogger(ReadCassandraExamples.getClass)) {
  val logger = LoggerFactory.getLogger(ReadCassandraExamples.getClass)

  val prepCsvTicks = session.prepare(
    """ select db_tsunx,ask from mts_src.ticks where ticker_id = :tickerId allow filtering """)

  val prepCsvBars = session.prepare(
    """ select ts_begin,ts_end,o,h,l,c,log_co,disp,ticks_cnt from mts_bars.bars where ticker_id = :tickerId and bar_width_sec = :p_bar_width_sec allow filtering """)

  val prepTickers = session.prepare(""" select ticker_id,ticker_code from mts_meta.tickers """) //where ticker_id=1 allow filtering

  val tickersSymbols : Seq[Tickers] = JavaConverters.asScalaIteratorConverter(session.execute(prepTickers.bind()).all().iterator())
                                                    .asScala.toSeq
                                                    .map(r => Tickers(r.getInt("ticker_id"),r.getString("ticker_code"))  )
                                                    .sortBy(_.ticker_id).toList

  def upload(FileName :String,tickerID :Int)={
    logger.info("BEGIN UPLOAD.")

    import java.io.{BufferedWriter, FileWriter}

    val out = new BufferedWriter(new FileWriter("C:\\Users\\Yakushev\\Desktop\\TEST_BC\\"+FileName+".csv"))
    val writer = new CSVWriter(out,';')

    val ticksSchema = Array("ts","rn", "prc")
    val bondPrepCsvTicks =  prepCsvTicks.bind().setInt("tickerId", tickerID)
    val dsTickersWidths =  JavaConverters.asScalaIteratorConverter(session.execute(bondPrepCsvTicks).all().iterator())
                                         .asScala.toSeq
                                         .map(r => CsvTick(r.getLong("db_tsunx"),r.getDouble("ask"))  )
                                         .sortBy(_.ts).toList
    val listOfRecords = new java.util.ArrayList[Array[String]]()
    listOfRecords.add(ticksSchema)

    for ((rec,idx) <- dsTickersWidths.zipWithIndex){
      listOfRecords.add(Array(rec.ts.toString, (idx+1).toString, rec.prc.toString.replace('.',',')))
    }

    writer.writeAll(listOfRecords)
    writer.flush()
    writer.close()

    /*
    output images.

    val ticksTS = for ((rec,idx) <- dsTickersWidths.zipWithIndex) yield (idx.toDouble,rec.prc)

    val x :Seq[Double] = ticksTS.map(t => t._1)
    val y :Seq[Double] = ticksTS.map(t => t._2)

    val series = new MemXYSeries(x, y)
    val data = new XYData(series)
    //save ticks as graph
    val fn = FileName+"_TICKS"
    output(PNG("C:\\Users\\Yakushev\\Desktop\\TEST_BC/", fn), xyChart(
         data,
         fn
        ))
    */

    for (w <- List(30,300,600)) {
      //------------------------------------------------------------------
      val out = new BufferedWriter(new FileWriter("C:\\Users\\Yakushev\\Desktop\\TEST_BC\\"+FileName+"_BARS_"+w+".csv"))
      val writerBars = new CSVWriter(out,';')

      val barsSchema = Array("ts_begin","ts_end","rn","o","h","l","c","log_co","disp","ticks_cnt")

      val bondPrepCsvBars =  prepCsvBars.bind().setInt("tickerId", tickerID)
                                                .setInt("p_bar_width_sec",w)

      val dsBarsWidths =  JavaConverters.asScalaIteratorConverter(session.execute(bondPrepCsvBars).all().iterator())
                                        .asScala.toSeq
                                        .map(r => CsvBar(r.getLong("ts_begin"),
                                                         r.getLong("ts_end"),
                                                         r.getDouble("o"),
                                                         r.getDouble("h"),
                                                         r.getDouble("l"),
                                                         r.getDouble("c"),
                                                         r.getDouble("log_co"),
                                                         r.getDouble("disp"),
                                                         r.getInt("ticks_cnt"))
                                            )
                                        .sortBy(_.ts_begin).toList
      val listOfRecords = new java.util.ArrayList[Array[String]]()
      listOfRecords.add(barsSchema)

      for ((rec,idx) <- dsBarsWidths.zipWithIndex){
        listOfRecords.add(Array(rec.ts_begin.toString,
                                rec.ts_end.toString,
                               (idx+1).toString,
                                rec.o.toString.replace('.',','),
                                rec.h.toString.replace('.',','),
                                rec.l.toString.replace('.',','),
                                rec.c.toString.replace('.',','),
                                rec.log_co.toString.replace('.',','),
                                rec.disp.toString.replace('.',','),
                                rec.ticks_cnt.toString))
      }

      writerBars.writeAll(listOfRecords)
      writerBars.flush()
      writerBars.close()
      //------------------------------------------------------------------
    }

    logger.info("END UPLOAD.")
  }


  def makeGraphImage(FileName :String,tickerID :Int)={
    val x = 0.0 until 2.0 * math.Pi by 0.1
    output(PNG("C:\\Users\\Yakushev\\Desktop\\TEST_BC/", "test"), xyChart(  x ->(math.sin(_))  )         )
  }


}

object CsvUploader extends App {
  val du = new DataUploader(new bar.calculator.SimpleClient("127.0.0.1").session)
  du.tickersSymbols.map(tsmb => du.upload(tsmb.ticker_code,tsmb.ticker_id))
  //du.makeGraphImage("eurusd.png",1)
}


