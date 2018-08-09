import java.util.Date

import bar.calculator.BarC
//import bar.calculator.BarC
import com.datastax.driver.core.{Row, Session}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

case class TendBarsMnMx(ticker_id : Int, width_sec : Int, deep_sec :Int, bars_cnt :Long, ts_begin :Long, ts_end :Long){
  val interval_begin_end_sec = ts_end - ts_begin

  val from_ts = ts_end - deep_sec

  override def toString = {
    "ticker_id =" +ticker_id +" width_sec="+ width_sec +" deep_sec="+ deep_sec +" bars_cnt="+ bars_cnt +" ts_begin="+ ts_begin +" ts_end="+ ts_end +" interval_begin_end_sec="+ interval_begin_end_sec+ "sec.  SPLIT from_ts="+from_ts
  }

}







object First extends App {
  val logger = LoggerFactory.getLogger(First.getClass)
  logger.info("BEGIN APPLICATION.")
  val client = new bar.calculator.SimpleClient("127.0.0.1")

  val session = client.session

  val query_mnmx = """select count(ticker_id)  as bars_cnt,
                               min(ts_begin)    as ts_begin,
                               max(ts_end)      as ts_end
                        from mts_bars.bars
                       where ticker_id     = :p_ticker_id and
                             bar_width_sec = :p_width_sec
                       allow filtering """

  val prep_query_ = session.prepare(query_mnmx)

  val query_bars = """      select
                                  ticker_id,
                                  ddate,
                                  bar_width_sec,
                                  ts_begin,
                                  ts_end,
                                  o,
                                  h,
                                  l,
                                  c,
                                  h_body,
                                  h_shad,
                                  btype,
                                  ticks_cnt,
                                  disp,
                                  log_co
                             from mts_bars.bars
                             where
                                  ticker_id     = :p_ticker_id and
                                  bar_width_sec = :p_width_sec and
                                  ts_begin     >= :p_ts_begin and
                                  ts_end       <= :p_ts_end
                             allow filtering;  """

  val prep_query_bars = session.prepare(query_bars)


  /**
    *
    * @param session
    * @param p_ticker_id
    * @param p_width_sec
    * @param p_deep_sec
    * @return
    */
  def get_bars_mnmx_tick_tend_1_conf(session: Session, p_ticker_id :Int, p_width_sec : Int, p_deep_sec :Int) : Seq[TendBarsMnMx] = {
    val rowTo_TendBarsMnMx = (row: Row) => {
      TendBarsMnMx(
        p_ticker_id, p_width_sec, p_deep_sec,
        row.getLong("bars_cnt"),
        row.getLong("ts_begin"),
        row.getLong("ts_end"))}
    val bond_query_ =  prep_query_.bind().setInt("p_ticker_id", p_ticker_id)
                                         .setInt("p_width_sec", p_width_sec)
    val rsTendBarsMnMx :Seq[TendBarsMnMx] = JavaConverters.asScalaIteratorConverter(session.execute(bond_query_).all().iterator())
                                                   .asScala.toSeq.map(rowTo_TendBarsMnMx)
    rsTendBarsMnMx
  }


  /**
    *
    * @param session
    * @param p_ticker_id
    * @param p_width_sec
    * @param p_ts_begin
    * @param p_ts_end
    * @return
    */
  def get_bars_by_ts_interval(session: Session, p_ticker_id :Int, p_width_sec : Int, p_ts_begin :Long, p_ts_end :Long) : Seq[BarC] = {
    val rowToBar = (row : Row) => {
      new BarC(
        row.getInt("ticker_id"),
        new Date(row.getDate("ddate").getMillisSinceEpoch),
        row.getInt("bar_width_sec"),
        row.getLong("ts_begin"),
        row.getLong("ts_end"),
        row.getDouble("o"),
        row.getDouble("h"),
        row.getDouble("l"),
        row.getDouble("c"),
        row.getDouble("h_body"),
        row.getDouble("h_shad"),
        row.getString("btype"),
        row.getInt("ticks_cnt"),
        row.getDouble("disp"),
        row.getDouble("log_co")
      )}

    logger.info("---------------------------------------")
    logger.info("")
    logger.info("   p_ts_begin="+p_ts_begin+"   p_ts_end="+p_ts_end)
    logger.info("")
    logger.info("---------------------------------------")

    val bond_bars_query_ =  prep_query_bars.bind().setInt("p_ticker_id", p_ticker_id)
                                                  .setInt("p_width_sec", p_width_sec)
                                                  .setLong("p_ts_begin",p_ts_begin)
                                                  .setLong("p_ts_end",p_ts_end)
    val rsBars :Seq[BarC] = JavaConverters.asScalaIteratorConverter(session.execute(bond_bars_query_).all().iterator())
      .asScala.toSeq.map(rowToBar).sortBy(t => t.ts_end)
    rsBars
  }




 //==================================================================
  // read bar property and execute next for all bp.

  val tendRes : Seq[TendBarsMnMx] = {for {
                                          (l_ticker_id,l_width_sec, l_deep_sec) <- List((1,600, 3600)/*,(1,300,3600)*/)
                                          thisTendRes : TendBarsMnMx  <- get_bars_mnmx_tick_tend_1_conf(session, l_ticker_id, l_width_sec, l_deep_sec)
                                         } yield thisTendRes
                                    }.filter(r => (r.interval_begin_end_sec >= r.deep_sec))

  for (tr <- tendRes) println(tr)

  val barsList : Seq[BarC] = for {
                                  barMnMx        <- tendRes
                                  thisBar : BarC <- get_bars_by_ts_interval(session, barMnMx.ticker_id, barMnMx.width_sec, barMnMx.from_ts, barMnMx.ts_end)
                                 } yield thisBar

  //println("barsList.size="+barsList.size+"  first(ts_begin)="+barsList.head.ts_begin+"  last(ts_end)="+barsList.last.ts_end)
  for (b <- barsList){
    logger.info(" begin - end :    "+b.ts_begin+" - "+b.ts_end)
  }

  //divide full sequnce of Bars on 3 parts and calculate Ro.

  logger.info("END APPLICATION.")
}



