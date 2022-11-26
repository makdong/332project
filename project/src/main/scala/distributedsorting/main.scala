package distributedsorting

import org.apache.log4j.Logger

object main extends App {
  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("main program starts")

  def main(): Unit = {
    logger.trace("trace1")
    logger.debug("debug1")
    logger.info("info1")
    logger.warn("warn1")
    logger.error("error1")

    print("Hello World!")
  }

  main()
}

