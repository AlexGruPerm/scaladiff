import bar.{ReadCassandraExamples, rowToX}
import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters


class PatternSearcher(session: Session) extends rowToX(session, LoggerFactory.getLogger(ReadCassandraExamples.getClass)) {
  val logger = LoggerFactory.getLogger(ReadCassandraExamples.getClass)

  // How much bars in pattern, taken from last
  def calc(deepCntPattern :Int)={
    logger.info("Begin pattern searcher.")

    val tickersProps :Seq[TickerProperty]  = JavaConverters.asScalaIteratorConverter(session.execute(prepTickersWidths.bind()).all().iterator())
      .asScala.toSeq.map(r => new TickerProperty(r.getInt("ticker_id"),
                                                 r.getInt("bar_width_sec"),
                                                 deepCntPattern)
                        )
      .sortBy(_.ticker_id).toList.filter(tp => tp.ticker_id==1 && tp.bar_width_sec==30)

    for (tp <- tickersProps){
      logger.info("PatternSearcher 1. ticker="+tp.ticker_id+" bar_width_sec="+tp.bar_width_sec+"   fullSeqBars="+tp.seqBars.size+"   getSearchArea="+tp.getSearchArea.size)

      for ((bp,idx) <- tp.getPatternForSearch.getSeqBarCpat.zipWithIndex) {
        logger.info(" index=  "+idx+" "+ bp.b.btype+" "+ bp.b.ts_end +
          " prcntTicks (" + bp.ticks_cnt_prcnt_from + " - " +bp.ticks_cnt_prcnt_to+") "+
          " prcntLogCO ("+bp.abs_logco_prcnt_from+" - "+bp.abs_logco_prcnt_to+")"
          )
      }

      logger.info("----------")
      logger.info("searchArea.size="+tp.getSearchArea.size+" LAST ts_end="+tp.getSearchArea.last.ts_end)

      //val searchRes :Seq[BarC] =
       val res = tp.getSearchResult

      logger.info("res.size="+res.size)
       for(r <- res) {
         logger.info("r.size="+r.size+ "  ts_begin "+r.head.ts_end+"  ts_end "+r.last.ts_end)
       }

    }

/*
  val seqBarAnalyzed :Seq[BarFutureAnal] =
      for (
       tw <- tickersWidths;
       seqB <- tw.getSeqBars
      )
      yield {
        analyzeThisBar(seqB)
      }
*/

    logger.info("End pattern searcher.")
  }

}

object PatSearchRunner extends App {
  val ps = new PatternSearcher(new bar.calculator.SimpleClient("127.0.0.1").session)
  ps.calc(4)
}
