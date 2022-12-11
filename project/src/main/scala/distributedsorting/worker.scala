package src.main.scala.distributedsorting

import distributedsorting.connection._
import io.grpc._
import java.net._
import java.io._
object worker {
    def main(args: Array[String]): Unit = {
        val masterIp = args(0).split(":")(0)
        val masterPort = args(0).split(":")(1).toInt
        def parsing(option: String, optionMap: (List[String], List[String]), argList: List[String]): (List[String], List[String]) = {
            if (argList.size == 0) {
                optionMap
            } else {
                argList.head match {
                    case "-I" => {
                        parsing("-I", optionMap, argList.tail)
                    }
                    case "-O" => {
                        parsing("-O", optionMap, argList.tail)
                    }
                    case x: String => {
                        option match {
                            case "-I" => {
                                parsing(option, (x :: optionMap._1, optionMap._2), argList.tail)
                            }
                            case "-O" => {
                                parsing(option, (optionMap._1, x :: optionMap._2), argList.tail)
                            }
                            case _ => {
                                print("check option")
                                scala.sys.exit(0)
                            }
                        }
                    }
                }
            }
        }

        val optionMap = parsing("", (List(), List()), args.toList.tail)
        val inputDirectorys = optionMap._1
        val outputDirectorys = optionMap._2

        val workerIp:String = InetAddress.getLocalHost.getHostAddress
        val workerPort:Int = 8888

        val client = new ConnectionClient(masterIp, masterPort, workerIp, workerPort)

        try{
            client.connectRequest

            //client.sort

            client.sortRequest

            //client.sample

            //client.key = ???

            client.sampleRequest

            client.shuffling

            client.shutdown(true)
        }catch{
            case e: Exception => println(e)
        }finally{
            client.shutdown(false)
        }
    }
}
