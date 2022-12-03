package distributedsorting

import org.apache.log4j.Logger
import io.grpc.{Server, ServerBuilder}
import distributedsorting.connection.{CheckWorkersRunningGrpc, ConnectionRequest, ConnectionRespond}
import distributedsorting.distribute.{DistributeBlocksGrpc, DistributeRequest, DistributeRespond}
import distributedsorting.shuffle.{GiveCriteriaGrpc, ShuffleRequest, ShuffleRespond}
import distributedsorting.merge.{MergeBlockGrpc, MergeRequest, MergeRespond}

import scala.concurrent.{ExecutionContext, Future}
object master extends App {
  def isIPValid(isPort: Boolean, ips: Array[String]): Boolean = {
    try{
      var ip_array = Array[String]()
      var port_array = Array[Int]()
      var temp_ip:String = null
      var temp_port:Int = null
      for(ip <- strings) {
        if(isPort) {
          if(ip_array.isEmpty && port_array.isEmpty) {
            temp_ip = ip.split(":").apply(0)
            temp_port = ip.split(":").apply(1).toInt
            var temp_ip_array = temp_ip.split(".")
            if(temp_ip_array.length != 4){
              throw Invalid
            }
            temp_ip_array.foreach(ip_num => if (ip_num.toInt < 0 || ip_num.toInt >255) throw Invalid)
            ip_array +:= temp_ip
            port_array +:= temp_port
          }
          else {
            temp_ip = ip.split(":").apply(0)
            temp_port = ip.split(":").apply(1).toInt
            var temp_ip_array = temp_ip.split(".")
            if(temp_ip_array.length != 4){
              throw Invalid
            }
            temp_ip_array.foreach(ip_num => if (ip_num.toInt < 0 || ip_num.toInt >255) throw Invalid)
            ip_array.foreach(past_ip => if(past_ip != temp_ip) throw Invalid)
            port_array.foreach(past_port => if(past_port == temp_port) throw Invalid)
            port_array +:= temp_port
          }
        } 

        else {
          if(ip_array.isEmpty) {
            var temp_ip_array = ip.split(".")
            if(temp_ip_array.length != 4){
              throw Invalid
            }
            temp_ip_array.foreach(ip_num => if (ip_num.toInt < 0 || ip_num.toInt >255) throw Invalid)
            ip_array +:= temp_ip
          }
          else {
            var temp_ip_array = ip.split(".")
            if(temp_ip_array.length != 4){
              throw Invalid
            }
            temp_ip_array.foreach(ip_num => if (ip_num.toInt < 0 || ip_num.toInt >255) throw Invalid)
            ip_array.foreach(past_ip => if(past_ip == temp_ip) throw Invalid)
            port_array +:= temp_port
          }
        }
      }
    }
    catch{
      case Invalid: false
    }
    true
  }


  def sortKeyMedian(keyMedian: List[String]): List[String] = {
    keyMedian.sorted
  }
  def calcWorkerOrder(keyMedian: List[String], keyMedianSorted: List[String]): List[Int] = {
    keyMedianSorted map (key => (keyMedian indexOf key) + 1)
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
        val worker_num = args_array.apply(1).toInt
        val ip_array = args_arrry.slice(2,args_length)
        assert(isIPValid(true, ip_array), "The format of IP address is invalid. Stop the program.")
        logger.info("IP Addresses and Port are Valid")
      }
      else {
        val worker_num = args_array.apply(0).toInt
        val ip_array = args_arrry.slice(1,args_length)
        assert(isIPValid(false, ip_array), "The format of IP address is invalid. Stop the program.")
        logger.info("IP Addresses and Port are Valid")
      }
    } catch {
      e => logger.error(e)
    }
  }


}