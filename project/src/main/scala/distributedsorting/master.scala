package distributedsorting

import org.apache.log4j.Logger
object master extends App {
  def isIPValid(isPort: Boolean, strings: Array[String]): Boolean = {
    false
  }

  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("main program starts")

  if (args.isEmpty) {
    logger.info("No arguments. Test mode with hard coded IP address starts.")
    val worker_num = 10
    val masterPort = 8023
    val workerPort = List(
      "8024",
      "8025",
      "8026",
      "8027",
      "8028",
      "8029",
      "8030",
      "8031",
      "8032",
      "8033",
    )
    val IPaddress = "141.223.232.63"
  } else {
    logger.info("Get arguments.")

    try {
      assert(args.length != 1, "Args.length is 1. There is no worker's IP address. Stop the program.")
      
      val args_array = args.split(" ")
      val args_length = args_array.length
      if (args_array.apply(0) == "-p") then {
        val worker_num = args_array.apply(1).toInt - 48
        val ip_array = args_arrry.slice(2,args_length)
        assert(isIPValid(true, ip_array), "The format of IP address is invalid. Stop the program.")
        logger.info("IP Addresses and Port are Valid")
      }
      else {
        val worker_num = args_array.apply(0).toInt - 48
        val ip_array = args_arrry.slice(1,args_length)
        assert(isIPValid(false, ip_array), "The format of IP address is invalid. Stop the program.")
        logger.info("IP Addresses and Port are Valid")
      }
    } catch {
      e => logger.error(e)
    }
  }
}
