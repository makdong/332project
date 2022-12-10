package src.main.scala.distributedsorting

import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import scala.annotation.tailrec

import scala.concurrent.{Promise}

import io.grpc.{ManagedChannelBuilder, Status}
import distributedsorting.connection._

class ConnectionClient {
    val logger: Logger = Logger.getLogger(classOf[ConnectionClient].getName)
    //logger.setLevel(loggerLevel.level)

    val channel = ManagedChannelBuilder.forAddress().usePlaintext.build
    val blockingStub = ConnectionGrpc.blockingStub(channel)
    val asyncStub = ConnectionGrpc.stub(channel)

    var id:Int = -1
    var workerNum:Int = -1

    def shutdown(success:Boolean): Unit = {
        logger.info("Client is Shutting Down")

        if(id != -1) {
            val message = new TerminateRequest(id, true)
            val response = blockingStub.terminate(message)
            id = -1
            channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
        }
    }

    def connectRequest():Unit = {
        logger.info("Client is Connecting to Master")
        val response = blockingStub.connect(new ConnectionRequest())
        id = response.id
    }
    @tailrec
    def sampleRequest():Unit = {
        logger.info("Client is requesting every key")
        val response = blockingStub.sample(new SamplingRequest)
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