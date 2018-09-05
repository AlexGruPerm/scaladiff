import bar.calculator.BarFutAnalRes
import bar.{PatternSearcherCommon, ReadCassandraExamples}
import com.datastax.driver.core.{Cluster, Session}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters
import scala.collection.JavaConverters._


class PatternSearcher(session: Session) extends PatternSearcherCommon(session, LoggerFactory.getLogger(ReadCassandraExamples.getClass)) {
  val logger = LoggerFactory.getLogger(ReadCassandraExamples.getClass)

  // How much bars in pattern, taken from last
  def calc(deepCntPattern :Int)={
    logger.info("Begin pattern searcher.")

    val tickersProps :Seq[TickerProperty]  = JavaConverters.asScalaIteratorConverter(session.execute(prepTickersWidths.bind()).all().iterator())
      .asScala.toSeq.map(r => new TickerProperty(r.getInt("ticker_id"),
                                                 r.getInt("bar_width_sec"),
                                                 deepCntPattern)
                        )
      .sortBy(_.bar_width_sec).toList.filter(tp => tp.ticker_id==11 && tp.bar_width_sec==30)

    for (tp <- tickersProps){
      //logger.info("   ticker="+tp.ticker_id+" width_sec="+tp.bar_width_sec+"   fullSeqBars="+tp.seqBars.size/*+"   getSearchArea="+tp.searchArea.size*/)

      tp.patternForSearch match {
        case pfs: PatternForSearchCls => {
          for ((bp, idx) <- pfs.getSeqBarCpat.zipWithIndex) {

           // Temporary closed.
            logger.info(" index=  " + idx + " " + bp.b.btype + "  ticksCnt=" + bp.b.ticks_cnt + " tsEnd=" + bp.b.ts_end +
              " prcntTicks (" + bp.ticks_cnt_prcnt_from + " - " + bp.ticks_cnt_prcnt_to + ") " +
              " prcntLogCO (" + bp.abs_logco_prcnt_from + " - " + bp.abs_logco_prcnt_to + ")"
            )

          }
        }
        case None => logger.info("patternForSearch is Empty")
      }

      //logger.info(/*"searchArea.size="+tp.searchArea.size+*/"      LAST ts_end="+{if (tp.searchArea.nonEmpty) tp.searchArea.last.ts_end})

      //val searchRes :Seq[BarC] =
       val res = tp.getSearchResult

      //next need analyze FutureSeacher results for making predictions !!!!!!
        logger.info("   ticker="+tp.ticker_id+" width_sec="+tp.bar_width_sec+"   fullSeqBars="+tp.seqBars.size+ " RES = "+res.size+" "+{if (res.size != 0) "  ***  " else " "})

      /* COMBINE ALL ANALYSIS results and input parameters INTO new Class For next saving into db.*/

      /*
      Temporary closed.
       */

       for(r <- res) {
         logger.info("    size="+r.size+ "           ts_begin "+r.head.ts_begin+"  ts_end "+r.last.ts_end)
       }


      val lst = res.map(ri => ri.last).map(r => r.ts_end.asInstanceOf[java.lang.Long]).toList

      logger.info("lst.size="+lst.size)

      val resFromFutAnal :Seq[BarFutAnalRes] = tp.getFutAnalResultsByTsEnds(lst.asJava)

      logger.info("resFromFutAnal.size = "+resFromFutAnal.size)

      for (rfa <- resFromFutAnal) {
        logger.info(rfa.toString)
      }

      //logger.info("-----------------------------------------------")
    }

    logger.info("End pattern searcher.")
  }

}

object PatSearchRunner extends App {
  private val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect()

  val ps = new PatternSearcher(session)
  ps.calc(4)
}
