
package src.main.scala.distributedsorting

import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import scala.annotation.tailrec

import scala.concurrent.{Promise}

import io.grpc.{ManagedChannelBuilder, Status}
import distributedsorting.connection._
import distributedsorting.sampling._

class ConnectionClient(masterIp:String, masterPort:Int, workerIp:String, workerPort:Int) {
    val logger: Logger = Logger.getLogger(classOf[ConnectionClient].getName)
    //logger.setLevel(loggerLevel.level)

    val channel = ManagedChannelBuilder.forAddress(masterIp, masterPort).usePlaintext.build
    val blockingStub = ConnectionGrpc.blockingStub(channel)
    val asyncStub = ConnectionGrpc.stub(channel)

    var id:Int = -1
    var workerNum:Int = -1
    var key:String = null

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

    def sort():Unit ={

    }

    @tailrec
    def sampleRequest():Unit = {
        logger.info("Client is requesting every key")
        val response = blockingStub.sample(new SamplingRequest(id, key))
        response.state match {
            case 1 => {

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
    def sample():Unit={

    }

    def shuffle():Unit={
        logger.info("Client starts Shuffling")
    }

    def merge():Unit={
        logger.info("Client starts Merging")
    }
}
