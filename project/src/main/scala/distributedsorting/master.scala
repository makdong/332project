package distributedsorting

import org.apache.log4j.Logger
import io.grpc._
import distributedsorting.connection._
import distributedsorting.distribute._
import distributedsorting.shuffle._
import distributedsorting.merge._
import java.net._

import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.TimeUnit

object Invalid extends Exception {}
object master extends App {
  def main(args: Array[String]): Unit = {
    assert(args.length < 1 || args(0).toInt > 0, "Not Enough Arguemts")
    val workerNum = args(0).toInt
    val port = {
      if (args.length == 2) {
        args(1).toInt
      }
      else {
        1343
      }
    }
    val server = new ConnectionServer(ExecutionContext.global, port, workerNum)

    try{
      server.start
      server.blockUntilShutdown
    } catch {
      case e: Exception => println(e)
    }
    finally {
      server.stop
    }
  }
  */
  /*
  def isIPValid(isPort: Boolean, ips: Array[String]): Boolean = {
    try {
      var ip_array = Array[String]()
      var port_array = Array[Int]()
      var temp_ip: String = null
      var temp_port: Int = -1
      for (ip <- ips) {
        if (isPort) {
          if (ip_array.isEmpty && port_array.isEmpty) {
            temp_ip = ip.split(":").apply(0)
            temp_port = ip.split(":").apply(1).toInt
            var temp_ip_array = temp_ip.split(".")
            if (temp_ip_array.length != 4) {
              throw Invalid
            }
            temp_ip_array.foreach(ip_num => if (ip_num.toInt < 0 || ip_num.toInt > 255) throw Invalid)
            ip_array +:= temp_ip
            port_array +:= temp_port
          }
          else {
            temp_ip = ip.split(":").apply(0)
            temp_port = ip.split(":").apply(1).toInt
            var temp_ip_array = temp_ip.split(".")
            if (temp_ip_array.length != 4) {
              throw Invalid
            }
            temp_ip_array.foreach(ip_num => if (ip_num.toInt < 0 || ip_num.toInt > 255) throw Invalid)
            ip_array.foreach(past_ip => if (past_ip != temp_ip) throw Invalid)
            port_array.foreach(past_port => if (past_port == temp_port) throw Invalid)
            port_array +:= temp_port
          }
        }

        else {
          if (ip_array.isEmpty) {
            var temp_ip_array = ip.split(".")
            if (temp_ip_array.length != 4) {
              throw Invalid
            }
            temp_ip_array.foreach(ip_num => if (ip_num.toInt < 0 || ip_num.toInt > 255) throw Invalid)
            ip_array +:= temp_ip
          }
          else {
            var temp_ip_array = ip.split(".")
            if (temp_ip_array.length != 4) {
              throw Invalid
            }
            temp_ip_array.foreach(ip_num => if (ip_num.toInt < 0 || ip_num.toInt > 255) throw Invalid)
            ip_array.foreach(past_ip => if (past_ip == temp_ip) throw Invalid)
            port_array +:= temp_port
          }
        }
      }
    }
    catch {
      case Invalid => {
        false
      }
    }
    true
  }
  def main(): Unit = {

    val logger = Logger.getLogger(this.getClass.getName)
    logger.info("main program starts")

    //val channel = ManagedChannelBuilder.forAddress("localhost", 9000).usePlaintext().build()
    //val blockingStub = CheckWorkersRunningGrpc.blockingStub(channel)

    //val master = new master(channel, blockingStub)

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
    }
    else {
      logger.info("Get arguments.")

      try {
        assert(args.length != 1, "Args.length is 1. There is no worker's IP address. Stop the program.")

        val args_length = args.length
        if (args.apply(0) == "-p") {
          val worker_num = args.apply(1).toInt
          val ip_array = args.slice(2, args_length)
          assert(worker_num != ip_array.length, "Not Enough Arguemts")
          assert(isIPValid(true, ip_array), "The format of IP address is invalid. Stop the program.")
          logger.info("IP Addresses and Port are Valid")
        }
        else {
          val worker_num = args.apply(0).toInt
          val ip_array = args.slice(1, args_length)
          assert(worker_num != ip_array.length, "Not Enough Arguemts")
          assert(isIPValid(false, ip_array), "The format of IP address is invalid. Stop the program.")
          logger.info("IP Addresses and Port are Valid")
        }
      } catch {
        case e : Throwable => logger.error(e)
      }
    }
    /*
    try {
      master.checkWorkersRunning()
    } finally {
      master.shutdown()
    }
    */
  }
}


/*
class master private(private val channel: ManagedChannel, private val blockingStub: CheckWorkersRunningBlockingStub) {
  val logger = Logger.getLogger(this.getClass.getName)
  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def checkWorkersRunning(): Unit = {
    val request = ConnectionRequest(isMasterStart = true)
    try {
      val response = blockingStub.connection(request)
    }
    catch {
      case e: StatusRuntimeException =>
//        logger.error("RPC failed", e)
        logger.error("RPC failed: {0}", e)
    }
  }
}
*/