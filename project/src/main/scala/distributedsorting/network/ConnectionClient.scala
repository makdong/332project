
package src.main.scala.distributedsorting

import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import scala.annotation.tailrec
import scala.collection.mutable.Map
import scala.concurrent.{ExecutionContext, Future}

import scala.concurrent.{Promise}

import io.grpc.{ManagedChannelBuilder, Status, Server}
import distributedsorting.connection._
import src.main.scala.distributedsorting.workerInfo

class ConnectionClient(masterIp:String, masterPort:Int, workerIp:String, workerPort:Int) {
    val logger: Logger = Logger.getLogger(classOf[ConnectionClient].getName)
    //logger.setLevel(loggerLevel.level)

    val channel = ManagedChannelBuilder.forAddress(masterIp, masterPort).usePlaintext().build()
    val blockingStub = ConnectionGrpc.blockingStub(channel)
    val asyncStub = ConnectionGrpc.stub(channel)

    var id:Int = -1
    var workerNum:Int = -1
    var key:String = null
    val worker_map : Map[Int, workerInfo] = Map()
    var partition_Server:partitionServer = null
    var partition_list = List[List[String]]()
    var partition_to_send:List[List[String]] = null

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
        logger.info(s"${masterIp}:${masterPort}")
        val response = blockingStub.connect(new ConnectionRequest(workerIp, workerPort))
        id = response.id
        partition_Server = new partitionServer(ExecutionContext.global, workerPort+58, id)
    }

    def sortRequest():Unit = {
        logger.info("Client has finished sorting")
        val response = blockingStub.sort(new SortRequest(id))
        response.state match {
            case 1=>{
                logger.info("All Clients have finished sorting")
            }
            case 2 => {
                logger.info("Exception Occurred")
            }
            case _ => {
                Thread.sleep(5000)
                sortRequest
            }
        }
    }

    def sampleRequest():Unit = {
        logger.info("Client is requesting every key")
        val response = blockingStub.sample(new SamplingRequest(id, key))
        response.state match {
            case 1 => {
                workerNum = response.workerNum
                for(w <- response.workerInfo){
                    val worker_info = new workerInfo(w.id, w.ip, w.port)
                    worker_info.state = w.state
                    worker_info.key = w.key
                    worker_map(w.id) = worker_info
                }
                partition_Server.workerNum = workerNum
                partition_Server.start()
            }
            case 2 => {
                logger.info("Exception Occurred")
            }
            case _ => {
                Thread.sleep(5000)
                sampleRequest
            }
        }
    }
    def permissionRequest():Unit = {
        logger.info("Client is asking permission")
        partition_Server.partition_list = partition_to_send
        val response = blockingStub.shufflingPermission(new PermissionRequest(id))
        if(response.permission == id){
            logger.info("Client obtain permission")
        }
        else{
            Thread.sleep(5000)
            permissionRequest
        }
    }

    def returnRequest():Unit = {
        logger.info("Client is returning permission")
        val response = blockingStub.permissionReturn(new shufflingRequest(id))
    }

    def shuffling():Unit={
        logger.info("Client starts Shuffling")
        logger.info(s"${partition_to_send.size}")
        for{work_id <- ((id + 1) to workerNum)++(1 until id)}{
            logger.info(s"Client requesting partition from worker ${work_id}")
            var client:partitionClient = null
            try{
                val worker_i = worker_map(work_id)
                logger.info(s"${worker_i.port + 58}")
                client = new partitionClient(id, worker_i.ip, worker_i.port+58, workerNum, key)
                logger.info("here11111")
                client.requestShuffle
                logger.info("here22222")
                partition_list = partition_list.appended(TypeConverter.string2block(client.partition))
            }
            finally{
                if(client != null) {
                    client.shutdown
                }
            }
        }
    }

}
