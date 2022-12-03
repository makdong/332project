package distributedsorting

import org.apache.log4j.Logger
import io.grpc.{Server, ServerBuilder}
import distributedsorting.connection.{CheckWorkersRunningGrpc, ConnectionRequest, ConnectionRespond}
import distributedsorting.distribute.{DistributeBlocksGrpc, DistributeRequest, DistributeRespond}
import distributedsorting.shuffle.{GiveCriteriaGrpc, ShuffleRequest, ShuffleRespond}
import distributedsorting.merge.{MergeBlockGrpc, MergeRequest, MergeRespond}

import scala.concurrent.{ExecutionContext, Future}
object master extends App {
  def isIPValid(strings: Array[String]): Boolean = {
    false
  }

  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("main program starts")

  if (args.isEmpty) {
    logger.info("No arguments. Test mode with hard coded IP address starts.")
    val masterPort = 1234
    val workerPort = List(
      "111",
      "111",
      "111",
      "111",
      "111",
      "111",
      "111",
      "111",
      "111",
      "111",
    )
    val IPaddress = "111.222.333.444"
  } else {
    logger.info("Get arguments.")

    try {
      assert(args.length != 1, "Args.length is 1. There is no worker's IP address. Stop the program.")
      assert(isIPValid(args), "The format of IP address is invalid. Stop the program.")
    } catch {
      e => logger.error(e)
    }
  }
}