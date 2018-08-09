import com.datastax.driver.core.{Row, Session}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

case class TendBarsMnMx(ticker_id : Int, width_sec : Int, deep_sec :Int, bars_cnt :Long, ts_begin :Long, ts_end :Long)







object First extends App {
  val logger = LoggerFactory.getLogger(First.getClass)
  logger.info("BEGIN APPLICATION.")
  val client = new bar.calculator.SimpleClient("127.0.0.1")

  val session = client.session

  val query_mnmx = """select count(ticker_id) as bars_cnt,
                               min(ts_begin)    as ts_begin,
                               max(ts_end)      as ts_end
                        from mts_bars.bars
                       where ticker_id     = :p_ticker_id and
                             bar_width_sec = :p_width_sec
                       allow filtering """

  val prep_query_ = session.prepare(query_mnmx)


  def get_bars_by_ticker_tend_1_conf(session: Session, p_ticker_id :Int, p_width_sec : Int, p_deep_sec :Int):Seq[TendBarsMnMx] = {

    val rowTo_TendBarsMnMx = (row: Row) => {
      TendBarsMnMx(
        p_ticker_id, p_width_sec, p_deep_sec,
        row.getLong("bars_cnt"),
        row.getLong("ts_begin"),
        row.getLong("ts_end"))
    }

    val bond_query_ =  prep_query_.bind().setInt("p_ticker_id", p_ticker_id)
                                         .setInt("p_width_sec", p_width_sec)
    val rsTicks :Seq[TendBarsMnMx] = JavaConverters.asScalaIteratorConverter(session.execute(bond_query_).all().iterator())
                                                   .asScala.toSeq.map(rowTo_TendBarsMnMx)
    rsTicks
  }




  // read bar property and execute next for all bp.




  val tendRes = for {
    (l_ticker_id,l_width_sec, l_deep_sec) <- List((1,600,3600),(1,300,3600),(2,600,3600),(10,600,3600))
    thisTendRes   <- get_bars_by_ticker_tend_1_conf(session,l_ticker_id,l_width_sec,l_deep_sec)
  } yield thisTendRes

  for (tr <- tendRes){
    println(tr)
  }

  logger.info("END APPLICATION.")
}



