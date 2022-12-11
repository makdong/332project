
package src.main.scala.distributedsorting

import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import scala.annotation.tailrec
import scala.collection.mutable.Map
import scala.concurrent.{ExecutionContext, Future}

import scala.concurrent.{Promise}

import io.grpc.{ManagedChannelBuilder, Status}
import distributedsorting.connection._
import distributedsorting.sampling._
import network.workerInfo.{workerInfo}

class ConnectionClient(masterIp:String, masterPort:Int, workerIp:String, workerPort:Int) {
    val logger: Logger = Logger.getLogger(classOf[ConnectionClient].getName)
    //logger.setLevel(loggerLevel.level)

    val channel = ManagedChannelBuilder.forAddress(masterIp, masterPort).usePlaintext.build
    val blockingStub = ConnectionGrpc.blockingStub(channel)
    val asyncStub = ConnectionGrpc.stub(channel)

    var id:Int = -1
    var workerNum:Int = -1
    var key:String = null
    val worker_map = Map[Int, workerInfo]
    val partition_Server = null
    val partition_list = List()

    def shutdown(success:Boolean): Unit = {
        logger.info("Client is Shutting Down")

        if(id != -1) {
            val message = new TerminateRequest(id, success)
            val response = blockingStub.terminate(message)
            id = -1
            channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
        }
    }

    def connectRequest():Unit = {
        logger.info("Client is Connecting to Master")
        val response = blockingStub.connect(new ConnectionRequest(workerIp, workerPort))
        id = response.id
        partition_Server = new partition_Server(ExecutionContext.global, workerPort, id)
    }

    @tailrec
    def sortRequest():Unit = {
        logger.info("Client has finished sorting")
        val response = blockingStub.sort(new SortRequest(id))
        response.match{
            case 1=>{
                logger.info("All Clients have finished sorting")
            }
            case 2 => {
                logger.info("Exception Occured")
            }
            case _ => {
                Thread.sleep(5000)
                sortRequest
            }
        }
    }

    @tailrec
    def sampleRequest():Unit = {
        logger.info("Client is requesting every key")
        val response = blockingStub.sample(new SamplingRequest(id, key))
        response.state match {
            case 1 => {
                workerNum = response.workerNum
                for(worker <- response.workerInfo){
                    worker_info = new workerInfo(w.id, w.ip, w.port)
                    worker_info.state = w.state
                    worker_info.key = w.key
                    worker_map[worker.id] = worker_info
                }
                partition_Server.start()
            }
            case 2 => {
                logger.info("Exception Occured")
            }
            case _ => {
                Thread.sleep(5)
                sampleRequest
            }
        }
    }

    def shuffling():Unit={
        logger.info("Client starts Shuffling")
        for{work_id <- ((id + 1) to workerNum)++(1 until id)}{
            logger.info(s"Client requesting partition from worker ${work_id}")
            var client = null
            try{
                val worker_i = worker_map[work_id]
                client = new partitionClient(worker_i.ip, worker_i.port, id)
                client.requestShuffle
                partition_list.appended(client.partition)
            }
            finally{
                if(client != null) {
                    client.shutdown
                }
            }
        }
    }

}
