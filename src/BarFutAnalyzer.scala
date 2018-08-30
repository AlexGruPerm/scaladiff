import bar.calculator.BarC
import bar.{BarFutureAnal, ReadCassandraExamples, rowToX}
import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters


class BarFutureAnalyzer(session: Session) extends rowToX(session, LoggerFactory.getLogger(ReadCassandraExamples.getClass)){
  val logger = LoggerFactory.getLogger(ReadCassandraExamples.getClass)

  val tickersWidths  = JavaConverters.asScalaIteratorConverter(session.execute(prepTickersWidths.bind()).all().iterator())
                                     .asScala.toSeq.map(r => new TickersWidth(r.getInt("ticker_id"),
                                                                          r.getInt("bar_width_sec")
                                                                         ))
                                     .sortBy(_.ticker_id).toList
                                     .filter(r => (r.ticker_id == 1 || r.ticker_id == 2) && r.bar_width_sec == 30) // DEBUG PURPOSE


  /**
    * Make search in the Future for 1 fixed log.
    * @param restBars  rest of bars after current
    * @param logReturn : fixed log Return for plua and minus to current bar price (Close)
    * Return : (Double,Char,Long) - Close price, Result type:u,d ., ts_end of close bar.
    */
  def searchFut(currBar : BarC, restBars : Seq[BarC], logReturn : Double) =  {
    val uPrice :Double = Math.exp(Math.log(currBar.c) + logReturn)
    val dPrice :Double = Math.exp(Math.log(currBar.c) - logReturn)
    restBars.find(p => (uPrice > p.l && dPrice < p.h)) match {
      case Some(b) => {
        if (b.l > currBar.c) (uPrice,"u",b.ts_end)
        else if (b.h < currBar.c) (dPrice,"d",b.ts_end)
        else (0.toDouble,"n",0.toLong)
      }
      case None => (0.toDouble,"n",0.toLong)
    }
  }


  def analyzeThisBar(currBar :BarC)= {
    /**
      * restBars contains all bars for ticker and width_sec that has ts_begin less than ts_end of current input bar.
      */
    val restBars = tickersWidths.filter(tw => tw.ticker_id == currBar.ticker_id && tw.bar_width_sec == currBar.bar_width_sec)
                                .head.get_seqBars
                                .dropWhile((b :BarC) => {b.ts_begin < currBar.ts_end})
    val listOfLogs = Seq(0.0017, 0.0034, 0.0051)

    val resOfAnalyze = for(lg <- listOfLogs) yield searchFut(currBar,restBars,lg)

    new BarFutureAnal(
                      currBar.ticker_id,
                      currBar.bar_width_sec,
                      currBar.ts_end,
                      currBar.c,
                      //-----------------
                      resOfAnalyze(0)._3,
                      resOfAnalyze(0)._2,
                      resOfAnalyze(0)._1,
                      //-----------------
                      resOfAnalyze(1)._3,
                      resOfAnalyze(1)._2,
                      resOfAnalyze(1)._1,
                      //-----------------
                      resOfAnalyze(2)._3,
                      resOfAnalyze(2)._2,
                      resOfAnalyze(2)._1
                     )
  }

  def calc() = {
    tickersWidths.foreach(r => logger.info("1. ticker_id = "+r.ticker_id+" width_sec = "+r.bar_width_sec+
                                             " FROM max_ts_end = "+r.get_maxTsEnd+" READED "+r.get_seqBars.size+" BARS."))
    /*
    Идём по каждому бару с начала (самый старый) и для каждого делаем просмотр вперед для 3-х случаев, заполняя seq of BarFutureAnal
    в конце вставляем seq[BarFutureAnal] в таблицу  mts_bars.bars_future
    */

    val seqBarAnalyzed :Seq[BarFutureAnal] =
      for (
       tw <- tickersWidths;
       seqB <- tw.get_seqBars
      )
      yield {
        analyzeThisBar(seqB)
      }

    logger.info("seqBarAnalyzed.size="+seqBarAnalyzed.size)
    logger.info("1 seqBarAnalyzed.size="+seqBarAnalyzed.filter(r => r.ticker_id == 1).size)
    logger.info("2 seqBarAnalyzed.size="+seqBarAnalyzed.filter(r => r.ticker_id == 2).size)

  }

}



object BarFutAnalyzer extends App {
 val fa = new BarFutureAnalyzer(new bar.calculator.SimpleClient("127.0.0.1").session)
 fa.calc()
}
