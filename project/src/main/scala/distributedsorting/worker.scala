package src.main.scala.distributedsorting

import distributedsorting.connection._
import io.grpc._
import java.net._
import java.io._
import scala.collection.mutable.Map

object worker {
    def main(args: Array[String]): Unit = {
        val masterIp = args(0).split(":")(0)
        val masterPort = args(0).split(":")(1).toInt
        def parsing(option: String, optionMap: (List[String], List[String], List[String]), argList: List[String]): (List[String], List[String], List[String]) = {
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
                    case "-P" => {
                        parsing("-P", optionMap, argList.tail)
                    }
                    case x: String => {
                        option match {
                            case "-I" => {
                                parsing(option, (x :: optionMap._1, optionMap._2, optionMap._3), argList.tail)
                            }
                            case "-O" => {
                                parsing(option, (optionMap._1, x :: optionMap._2, optionMap._3), argList.tail)
                            }
                            case "-P" => {
                                parsing(option, (optionMap._1, optionMap._2, x :: optionMap._3), argList.tail)
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

        val optionMap = parsing("", (List(), List(), List()), args.toList.tail)
        val inputDirectorys: List[String] = optionMap._1
        val outputDirectorys = optionMap._2

        val workerIp:String = InetAddress.getLocalHost.getHostAddress
        val workerPort:Int = {
            if(optionMap._3.isEmpty){
                8888
            }
            else {
                optionMap._3.head.toInt
            }
        }

        val client = new ConnectionClient(masterIp, masterPort, workerIp, workerPort)
        var keyOrder : List[Int] = null
        var block4Merge : List[List[String]] = null

        try{
            client.connectRequest

            val blockList = inputDirectorys.foldLeft(List(): List[String])((list: List[String], e: String) => {
                FileManager.readFileAsBlock(e) ::: list
            })

            val sortedBlockList = blockList.map(workerUtil.sortBlock(_))

            client.sortRequest

            val perBlockKeyList = sortedBlockList.map(block => workerUtil.getMedianKeyFromListEntry(TypeConverter.block2EntryList(block)))
            val medianKey = workerUtil.getMedianKeyFromKeyList(perBlockKeyList)

            client.key = medianKey

            client.sampleRequest
            val keyMap:Map[Int, workerInfo] = client.worker_map
            var keyList: List[String] = List()
            for(worker_id <- (1 to keyMap.size)){
                keyList = keyList.appended(keyMap(worker_id).key)
            }

            keyOrder = workerUtil.getWorkerOrderUsingKey(keyList)

            val sortedKeyList = keyList.sorted

            val blockListListForShuffle = keyList map (
              currentKey => {
                  val keyIndex = keyList.indexOf(currentKey)

                  val minRange = {
                      if (keyIndex == 0) {
                          "          "
                      } else {
                          sortedKeyList(keyIndex - 1)
                      }
                  }
                  val maxRange = {
                      if (keyIndex == keyList.size - 1) {
                          "~~~~~~~~~~"
                      } else {
                          sortedKeyList(keyIndex)
                      }
                  }
                  workerUtil.entryListList2blockList(sortedBlockList.map(TypeConverter.block2EntryList(_)), minRange, maxRange)
              }
            )

            client.partition_to_send = blockListListForShuffle
            client.permissionRequest
            Thread.sleep(5000)
            client.shuffling

            block4Merge = client.partition_list

            val currentWorkerInRangeData = blockListListForShuffle(keyList.indexOf(medianKey))

            block4Merge = block4Merge.appended(currentWorkerInRangeData)

            client.returnRequest
            client.waitRequest

            workerUtil.merge(outputDirectorys.head, block4Merge.map(_.map(TypeConverter.Entry(_))))
            client.shutdown(true)
        }catch{
            case e: Exception => println(e)
        }finally{
            client.shutdown(false)
        }
    }
}
