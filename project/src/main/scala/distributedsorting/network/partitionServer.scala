package src.main.scala.distributedsorting

import java.util.logging.Logger
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.Map
import distributedsorting.shuffle._
import io.grpc._

import java.net.InetAddress

class partitionServer(executionContext: ExecutionContext, port: Int, id: Int) { self =>
    val logger: Logger = Logger.getLogger(classOf[partitionServer].getName)

    var server: Server = null
    var partition_list:List[List[String]] = null
    val request_map:Map[Int, String] = Map()
    var state = 0
    var workerNum = -1
    

    def start():Unit = {
        server = ServerBuilder.forPort(port).addService(ShuffleGrpc.bindService(new ShuffleImpl, executionContext)).build.start
        logger.info("Partition Server started, listening on " + port)
        println(s"${InetAddress.getLocalHost.getHostAddress}:${port}")
        sys.addShutdownHook {
            logger.info("Shutting down Partition server since JVM is shutting down")
            self.stop()
            logger.info("Server shut down")
        }
    }

    def stop(): Unit = {
        if (server != null) {
        server.shutdown().awaitTermination(5, TimeUnit.SECONDS)
        }
    }
    def blockUntilShutdown(): Unit = {
        if (server != null) {
        server.awaitTermination()
        }
    }
    class ShuffleImpl() extends ShuffleGrpc.Shuffle {
        override def shuffle(request: ShuffleRequest):Future[ShuffleResponse] = {
            logger.info("Test0")
            request_map.synchronized{
                if(request_map.size < workerNum-1) {
                    request_map(request.id) = request.keyMedians
                    if(request_map.size == workerNum -1){
                        state = 1;
                    }
                    logger.info("Test1")
                    logger.info(s"request id is : ${request.id}")
                    logger.info(s"request id - 1 is : ${request.id - 1}")
                    logger.info(s"partition list size is : ${partition_list.size}")
                    val data:String = TypeConverter.block2string(partition_list(request.id - 1))
                    logger.info(s"Size = ${data.size}")
                    logger.info(s"${data}")
                    Future.successful(new ShuffleResponse(1,id, data))
                }
                else {
                    Future.failed(throw InvalidStateException)
                }
            }
        }
    }
}