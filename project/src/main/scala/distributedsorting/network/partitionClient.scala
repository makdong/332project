package src.main.scala.distributedsorting

import distributedsorting.shuffle._

import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import java.io.File
import scala.concurrent.duration._
import io.grpc.{ManagedChannelBuilder, Status}

import java.net.InetAddress

class partitionClient(id: Int, host_ip: String, host_port: Int, workerNum: Int, key:String) {
    val logger: Logger = Logger.getLogger(classOf[partitionClient].getName)
    val builder = ManagedChannelBuilder.forAddress(host_ip, host_port)
        builder.maxInboundMessageSize(1<<30)
    val channel = builder.usePlaintext().build()

    val blockingStub = ShuffleGrpc.blockingStub(channel)
    val asyncStub = ShuffleGrpc.stub(channel)

    var partition: String = null

    def shutdown(): Unit = {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }

    def requestShuffle():Unit = {
        logger.info("Asking Partition to other workers")
        println(s"${host_ip}:${host_port}")
        println(s"${InetAddress.getLocalHost.getHostAddress}")
        val response: ShuffleResponse=blockingStub.shuffle(new ShuffleRequest(id, key))
        logger.info("Getting Response Done")
        partition = response.partition
        response.state match {
            case 1 => {
                logger.info(s"Done getting partition from worker ${response.id}}")
            }
            case _ => {
                logger.info("Exception Occurred")
            }
        }
    }
}