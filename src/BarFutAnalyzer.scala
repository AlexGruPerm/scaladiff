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

  def analyzeThisBar(bar :BarC) : BarFutureAnal ={
      //
      // Тут основные вычисления, сравнения, сверки и т.д.
      // Возвращаем результат анализа как объект BarFutureAnal
      // 1. Цикл по
    val sb = tickersWidths.filter(tw => tw.ticker_id == bar.ticker_id && tw.bar_width_sec == bar.bar_width_sec)
                 .map(twf => twf.get_seqBars).
    val workSb = sb.dropWhile((b: BarC) => {b.ts_begin == bar.ts_begin})
  }

  def calc() = {
    tickersWidths.foreach(r => logger.info("1. ticker_id = "+r.ticker_id+" width_sec = "+r.bar_width_sec+
                                             " FROM max_ts_end = "+r.get_maxTsEnd+" READED "+r.get_seqBars.size+" BARS."))
    /*
    Идём по каждому бару с начала (самый старый) и для каждого делаем просмотр вперед для 3-х случаев, заполняя seq of BarFutureAnal
    в конце вставляем seq[BarFutureAnal] в таблицу  mts_bars.bars_future
    */
    val seqBarAnalyzed :Seq[BarFutureAnal]  =
      for (
       tw <- tickersWidths;
       seqB <- tw.get_seqBars
      )
      yield {
        analyzeThisBar(seqB)
      }



  }

}



object BarFutAnalyzer extends App {
 val fa = new BarFutureAnalyzer(new bar.calculator.SimpleClient("127.0.0.1").session)
 fa.calc()
}
