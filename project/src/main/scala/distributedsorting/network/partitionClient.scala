package src.main.scala.distributedsorting

import distributedsorting.shuffle._
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import java.io.File
import scala.concurrent.duration._

import io.grpc.{ManagedChannelBuilder, Status}

class partitionClient(id: Int, host_ip: String, host_port: Int, workerNum: Int) {
    val logger: Logger = Logger.getLogger(classOf[partitionClient].getName)

    val channel = ManagedChannelBuilder.forAddress(host_ip, host_port).usePlaintext.build
    val blockingStub = ShuffleGrpc.blockingStub(channel)
    val asyncStub = ShuffleGrpc.stub(channel)

    var partition: String = null

    def shutdown(): Unit = {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }

    def requestShuffle():Unit = {
        logger.info("Asking Partiton to other workers")
        val response = asyncStub.connect(new ShuffleRequest(workerIp, workerPort))
        partition = response.partition
        reponse.state match {
            case 1 => {
                logger.info(s"Done getting partition from worker ${response.id}}")
            }
            case _ => {
                logger.info("Exception Occured")
            }
        }
    }
}