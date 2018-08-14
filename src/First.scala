import java.util.Date

import bar.calculator.BarC
import com.datastax.driver.core.{Row, Session}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters

case class TendBarsMnMx(ticker_id : Int, width_sec : Int, deep_sec :Int, bars_cnt :Long, ts_begin :Long, ts_end :Long){
  val intervalBeginEndSec = ts_end - ts_begin
  val fromTs = ts_end - deep_sec

  override def toString = {
    "ticker_id =" +ticker_id +" width_sec="+ width_sec +" deep_sec="+ deep_sec +" bars_cnt="+ bars_cnt +" ts_begin="+ ts_begin +" ts_end="+ ts_end +" interval_begin_end_sec="+ intervalBeginEndSec+ "sec.  SPLIT from_ts="+fromTs
  }
}


object First extends App {
  val logger = LoggerFactory.getLogger(First.getClass)
  logger.info("BEGIN APPLICATION.")
  val client = new bar.calculator.SimpleClient("127.0.0.1")

  val session = client.session

  val queryTickersWW =
    """ select ticker_id
          from mts_meta.bars_property
         where bar_width_sec = 600 and
               is_enabled    = 1
         allow filtering; """

  val prepqueryTickersWW = session.prepare(queryTickersWW)

  val queryMinMax = """select count(ticker_id)  as bars_cnt,
                               min(ts_begin)    as ts_begin,
                               max(ts_end)      as ts_end
                        from mts_bars.bars
                       where ticker_id     = :p_ticker_id and
                             bar_width_sec = :p_width_sec
                       allow filtering """

  val prepQuery = session.prepare(queryMinMax)

  val queryBars = """      select
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

  val prepQueryBars = session.prepare(queryBars)


  /**
    *
    * @param session
    * @param p_ticker_id
    * @param p_width_sec
    * @param p_deep_sec
    * @return
    */
  def getBarsMinMaxTickTendConf(session: Session, p_ticker_id :Int, p_width_sec : Int, p_deep_sec :Int) : Seq[TendBarsMnMx] = {
    val rowToTendBarsMinMax = (row: Row) => {
      TendBarsMnMx(
        p_ticker_id, p_width_sec, p_deep_sec,
        row.getLong("bars_cnt"),
        row.getLong("ts_begin"),
        row.getLong("ts_end"))}
    val bond_query_ =  prepQuery.bind().setInt("p_ticker_id", p_ticker_id)
                                         .setInt("p_width_sec", p_width_sec)
    val rsTendBarsMnMx :Seq[TendBarsMnMx] = JavaConverters.asScalaIteratorConverter(session.execute(bond_query_).all().iterator())
                                                   .asScala.toSeq.map(rowToTendBarsMinMax)
    rsTendBarsMnMx
  }

  def simpleRound6Double(valueD : Double) = {
    (valueD * 1000000).round / 1000000.toDouble
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
  def getBarsByTsInterval(session: Session, p_ticker_id :Int, p_width_sec : Int, p_ts_begin :Long, p_ts_end :Long) : Seq[BarC] = {
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

    logger.debug("---------------------------------------")
    logger.debug("")
    logger.debug("   p_ts_begin="+p_ts_begin+"   p_ts_end="+p_ts_end)
    logger.debug("")
    logger.debug("---------------------------------------")

    val bondBarsQuery =  prepQueryBars.bind().setInt("p_ticker_id", p_ticker_id)
                                                  .setInt("p_width_sec", p_width_sec)
                                                  .setLong("p_ts_begin",p_ts_begin)
                                                  .setLong("p_ts_end",p_ts_end)
    val rsBars :Seq[BarC] = JavaConverters.asScalaIteratorConverter(session.execute(bondBarsQuery).all().iterator())
      .asScala.toSeq.map(rowToBar).sortBy(t => t.ts_end)
    rsBars
  }


  /**
    *
    * @param p_seqBarInfo - Full seq of info
    * @param p_index      - can be: 0,1,2 - only 3 parts
    * @param p_btype      - p_btype can be: g,r,n
    */
  def getBtypeCntByTypeIndex(p_ticker :Int, p_seqBarInfo : List[(Int,Map[String,(Int,Double)])], p_index :Int, p_btype :String) = {
    val res = p_seqBarInfo.filter(sb => sb._1 == p_ticker)(p_index)._2.getOrElse(p_btype,(0,0.toDouble))._1
    logger.info(" INSIDE   []   :  p_ticker = "+p_ticker+"  index = "+p_index+" btype = "+p_btype+"   CNT = "+res )
    res
  }

  def getLogCoByTypeIndex(p_ticker :Int, p_seqBarInfo : List[(Int,Map[String,(Int,Double)])], p_index :Int, p_btype :String) = {
    val res = p_seqBarInfo.filter(sb => sb._1 == p_ticker)(p_index)._2.getOrElse(p_btype,(0,0.toDouble))._2
    logger.info(" INSIDE   []   :  p_ticker = "+p_ticker+"  index = "+p_index+" btype = "+p_btype+"   LOG_CO = "+res )
    res
  }





  //===================================================================================================================
  // read bar property and execute next for all bp.

  val tickersWWRes =  JavaConverters.asScalaIteratorConverter(session.execute(prepqueryTickersWW.bind()).all().iterator())
                     .asScala.toSeq.map(r => (r.getInt("ticker_id"),600,3*3600)).sortBy(_._1).toList

  val tendRes : Seq[TendBarsMnMx] = {for {
                                          (lTickerId, l_width_sec, l_deep_sec) <- tickersWWRes  // List((1,600, 3*3600),(2,600, 3*3600))
                                          thisTendRes : TendBarsMnMx  <- getBarsMinMaxTickTendConf(session, lTickerId, l_width_sec, l_deep_sec)
                                         } yield thisTendRes
                                    }.filter(r => (r.intervalBeginEndSec >= r.deep_sec))

  logger.info(" tendRes: ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  for (tr <- tendRes) logger.info(tr.toString)
  logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

  val barsList : Seq[BarC] = for {
                                  barMnMx        <- tendRes
                                  thisBar : BarC <- getBarsByTsInterval(session, barMnMx.ticker_id, barMnMx.width_sec, barMnMx.fromTs, barMnMx.ts_end)
                                 } yield thisBar

  for (b <- barsList) logger.info("ticker_id="+b.ticker_id+" begin - end :    "+b.ts_begin+" - "+b.ts_end+"  ["+b.btype+"] "+b.log_co)

  //divide full sequnce of Bars on 3 parts and calculate Ro.
  val seqSeqBars_Parts = barsList.sliding(6,6).toList

  logger.info(" seqSeqBars_Parts :#################################")
  for (b <- seqSeqBars_Parts){
    logger.info("ticker_id="+b.head.ticker_id+" parts : size="+b.size+" begin-end: "+b.head.ts_begin+"  "+b.last.ts_end)
  }
  logger.info("####################################################")

  val seqSeqBars_Parts_AddInfo = for (blck <- seqSeqBars_Parts) yield {
                                logger.info("      seqSeqBars_Parts_AddInfo > ticker_id = "+blck.head.ticker_id)
                                (blck.head.ticker_id,
                                Map(
                                    ("g",(blck.count(b => b.btype=="g"), simpleRound6Double(blck.filter(b => b.btype=="g").map(b => b.log_co).sum) )),
                                    ("r",(blck.count(b => b.btype=="r"), simpleRound6Double(blck.filter(b => b.btype=="r").map(b => b.log_co).sum) )),
                                    ("n",(blck.count(b => b.btype=="n"), simpleRound6Double(blck.filter(b => b.btype=="n").map(b => b.log_co).sum) ))
                                   ))
  }


  logger.info("---------------------------------------")
  logger.info("!!!!!!!!!!!  seqSeqBars_Parts_AddInfo.size="+seqSeqBars_Parts_AddInfo.size)
  logger.info("---------------------------------------")

  for (p <- seqSeqBars_Parts_AddInfo){
    logger.info(" [seqSeqBars_Parts_AddInfo]   ticker_id="+p._1+"  "+p.toString)
  }

  val wType = for (cticker <- seqSeqBars_Parts_AddInfo.map(bi => bi._1).distinct.sortBy(v => v)) yield {
      //1.Test on G.(Green - up bars)
    if (
      (Seq(2, 3) contains getBtypeCntByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 0, "g")) &&
        (Seq(3, 4) contains getBtypeCntByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 1, "g")) &&
        (Seq(4) contains getBtypeCntByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 2, "g"))
        &&
        (getLogCoByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 2, "g") >
          getLogCoByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 1, "g")) &&
        (getLogCoByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 1, "g") >
          getLogCoByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 0, "g"))
    ) {
      logger.info("GREEN WAY")
      (cticker,"g")
    }
    //2. Test on R.(Red - down bars)
    else if (
      (Seq(2, 3) contains getBtypeCntByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 0, "r")) &&
        (Seq(3, 4) contains getBtypeCntByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 1, "r")) &&
        (Seq(4) contains getBtypeCntByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 2, "r"))
        &&
        (getLogCoByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 2, "r") <
          getLogCoByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 1, "r")) &&
        (getLogCoByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 1, "r") <
          getLogCoByTypeIndex(cticker, seqSeqBars_Parts_AddInfo, 0, "r"))
    ) {
      logger.info("RED WAY")
      (cticker,"r")
    }
    else {
      logger.info("ELSE WAY")
      (cticker,"n")
    }
  }

  for (tr <- wType){
    logger.info("RES = "+tr)
  }

  /*
  if (Seq("g","r") contains wType) {
  //SAVE results into DB.
    logger.info("-- ---------------------------------- --")
    logger.info("                                        ")
    logger.info(" FOUND "+wType+" way                    ")
    logger.info(" SAVE ADVISE INTO DB FOR ticker_id="+seqSeqBars_Parts.head.head.ticker_id)
    logger.info("                                        ")
    logger.info("-- ---------------------------------- --")
  }
  */


}



