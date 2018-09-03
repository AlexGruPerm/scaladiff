import bar.{ReadCassandraExamples, rowToX}
import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

class PatternSearcher(session: Session) extends rowToX(session, LoggerFactory.getLogger(ReadCassandraExamples.getClass)) {
  val logger = LoggerFactory.getLogger(ReadCassandraExamples.getClass)

  def calc()={
    logger.info("Begin pattern searcher.")

  }

}

object PatSearchRunner extends App {
  val ps = new PatternSearcher(new bar.calculator.SimpleClient("127.0.0.1").session)
  ps.calc()
}
