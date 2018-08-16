import bar.{ReadCassandraExamples, rowToX}
import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

case class cClassBarsMinMax(seqBars: Seq[(Double, Double)]){
  val minPrc: Double = seqBars.map(b => b._1).min
  val maxPrc: Double = seqBars.map(b => b._2).max
}

class VsaCalc(session: Session) extends rowToX(session, LoggerFactory.getLogger(ReadCassandraExamples.getClass)) {
  val logger = LoggerFactory.getLogger(ReadCassandraExamples.getClass)

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  val queryTickerWidths =
    """ select ticker_id, bar_width_sec
          from mts_meta.bars_property
         where is_enabled = 1
               allow filtering """

  val prepQueryTickerWidths = session.prepare(queryTickerWidths)


  val queryBarsHL =
    """  select h,l
           from mts_bars.bars
          where ticker_id        = :p_ticker_id and
                   bar_width_sec = :p_width_sec
             allow filtering  """

  val prepQueryBarsHL = session.prepare(queryBarsHL)

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  def readBarsExtrMinMax(thisTickerId :Int, thisWidthSec :Int) : cClassBarsMinMax = {
    val bondQueryBarsHL =  prepQueryBarsHL.bind().setInt("p_ticker_id", thisTickerId)
                                                 .setInt("p_width_sec", thisWidthSec)
    cClassBarsMinMax(JavaConverters.asScalaIteratorConverter(session.execute(bondQueryBarsHL).all().iterator())
                                                             .asScala.toSeq.map(row => (row.getDouble("l"),
                                                                                        row.getDouble("h"))))
  }


  def simpleRound4Double(valueD : Double) = {
    (valueD * 10000).round / 10000.toDouble
  }





  def calc(pDivCount : Int)={
    logger.info("BEGIN VsaCalc calc()")
    //Get all tickers and width with enabled=1
    val dsTickersWidths =  JavaConverters.asScalaIteratorConverter(session.execute(prepQueryTickerWidths.bind()).all()
                                         .iterator())
                                         .asScala.toSeq
                                         .map(r => (r.getInt("ticker_id"), r.getInt("bar_width_sec")))
                                         .sortBy(_._1).toList.filter(deb => deb._1==1 && deb._2==30)

    //# debug
    for (elmTW <- dsTickersWidths) logger.debug(elmTW.toString())

    for((thisTickerId,thisWidthSec) <- dsTickersWidths) {

      val dsBarsInfoMinMax = readBarsExtrMinMax(thisTickerId, thisWidthSec)

      logger.info("ticker =" + thisTickerId + " and width = " + thisWidthSec +
                                              " (H,L)PAIRS Size="+ dsBarsInfoMinMax.seqBars.size +
                                              " minL = "+ dsBarsInfoMinMax.minPrc +
                                              " maxH = "+ dsBarsInfoMinMax.maxPrc)

      val rngStep = (dsBarsInfoMinMax.maxPrc - dsBarsInfoMinMax.minPrc)/pDivCount

      val rngPrc = (dsBarsInfoMinMax.minPrc to dsBarsInfoMinMax.maxPrc by rngStep).toList

      logger.debug(rngPrc.toString)

      val prcnt   :Double =  (rngPrc(1)-rngPrc(0))/10
      val barsCnt :Int = dsBarsInfoMinMax.seqBars.size

      /**
        * seqFreq : Seq[Double,Int,Int]
        * Middle Price
        * Freq common
        * Freq tails HL
        */
      val seqFreqInit : IndexedSeq[(Double,Int,Int)] = for (i <- 0 until rngPrc.size-1) yield {
        val rngMiddlePrc = (rngPrc(i) + rngPrc(i+1))/2
          (rngMiddlePrc,
           dsBarsInfoMinMax.seqBars.count(b => (rngMiddlePrc >= b._1 && rngMiddlePrc <= b._2 ) ),
           dsBarsInfoMinMax.seqBars.count(b => (( b._1 > rngMiddlePrc - prcnt && b._1 < rngMiddlePrc + prcnt) ||
                                                ( b._2 > rngMiddlePrc - prcnt && b._2 < rngMiddlePrc + prcnt)))
          )
      }

      val cntCommon :Int = seqFreqInit.map(sf => sf._2).sum
      val cntTails  :Int = seqFreqInit.map(sf => sf._3).sum


      logger.info("cntCommon="+cntCommon+"  cntTails="+cntTails)

      //save into db
      for (sf <- seqFreqInit){
        logger.info(thisTickerId+" "+thisWidthSec+" "+sf._1+"   "+sf._2+"    "+sf._3)
      }



     // logger.info("      ")
    }


  }
}








object VsaCalculator extends App {
  val vsaCalcInst = new VsaCalc(new bar.calculator.SimpleClient("127.0.0.1").session);
  vsaCalcInst.calc(100)
}


