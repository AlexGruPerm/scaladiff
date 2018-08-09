import org.slf4j.LoggerFactory

object First extends App {
  val logger = LoggerFactory.getLogger(First.getClass)
  logger.info("BEGIN APPLICATION.")

  val client = new bar.calculator.SimpleClient("127.0.0.1")

  val results = client.session.execute("select * from mts_meta.bars_property;")
  val rsList = results.all()
  val bp = for(i <- 0 to rsList.size()-1) yield {
    (
      rsList.get(i).getInt("ticker_id"),
      rsList.get(i).getInt("bar_width_sec"),
      rsList.get(i).getInt("is_enabled")
    )
  }

  for (thisBp <- bp){
    println(thisBp)
  }

}



