package src.main.scala.distributedsorting

import java.util.logging.Logger
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.Map

import distributedsorting.shuffle._

class partitionServer(executionContext: ExecutionContext, port: Int, id: Int) { self =>
    val logger: Logger = Logger.getLogger(classOf[partitionServer].getName)

    val server: Server = null
    var partition_list:String = null
    val request_map = Map[int, String]{}
    var state = 0
    

    def start():Unit = {
        server = ServerBuilder.forPort(port).addService(ConnectionGrpc.bindService(new ConnectionImpl, executionContext)).build.start
        logger.info("Partition Server started, listening on " + port)
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
        override def shuffle(request: ShuffleRequest):Future[ShuffleResponse] {
            request_map.synchronized{
                if(request_map.size < workerNum-1) {
                    request_map[request.id] = request.partition
                    if(request_map.size == workerNum -1){
                        state = 1;
                    }
                    Future.successful(new shuffleResponse(1,id,partition_list(request.id - 1)))
                }
                else {
                    Future.failed()
                }
            }
        }
    }
}