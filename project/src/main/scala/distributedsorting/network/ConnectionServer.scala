package src.main.scala.distributedsorting

import scala.concurrent.{ExecutionContext, Future, Promise, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.Map
import scala.concurrent.duration._
import java.net._

import java.util.concurrent.TimeUnit

import java.util.logging.Logger
import io.grpc.{Server, ServerBuilder, Status}
import distributedsorting.connection._

class ConnectionServer(executionContext: ExecutionContext, port: Int, workerNum: Int) { self=>
    val logger = Logger.getLogger(classOf[ConnectionServer].getName)
    //logger.setLevel(loggerLevel.level)

    var server:Server = null
    val worker_map = Map[Int, WorkerInfo]{}
    var state = 0

    def start():Unit = {
        server = ServerBuilder.forPort(port).addService(ConnectionGrpc.bindService(new ConnectionImpl, executionContext)).build.start
        logger.info("Server started, listening on " + port)
        println(s"${InetAddress.getLocalHost.getHostAddress}:${port}")
        sys.addShutdownHook {
            logger.info("Shutting down gRPC server since JVM is shutting down")
            self.stop()
            logger.info("Server shut down")
        }
    }

    def stop():Unit = {
        if (server != null) {
            server.shutdown.awaitTermination(5, TimeUnit.SECONDS)
        }
    }

    def blockUntilShutdown(): Unit = {
        if (server != null) {
            server.awaitTermination
        }
    }

    def check_All(m_state:Int, w_state:Int):Booelan = worker_map.synchronized{
        if (state == m_state && worker_map.size == workerNum && worker_map.forall {case (id, worker) => worker.state == w_state}) true
        else false
    }
    class ConnectionImpl() extends ConnectionGrpc.Connection {
        override def connect(request: ConnectionRequest): Future[ConnectionResponse] = {
            if (state != 0) {
                Future.failed()
            }
            else {
                logger.info(s"${request.ip}:${request.port}")
                worker_map.synchronized{
                    if(worker_map.size < workerNum) {
                        worker_map(worker_map.size + 1) = new workerInfo(worker_map.size + 1,request.ip, request.port)
                        if(worker_size == workerNum){
                            state = 1
                        }
                        Future.successful(new ConnectionResponse(worker_map.size))
                    }
                    else {
                        Future.failed()
                    }
                }
            }

        }

        override def terminate(request: TerminateRequest): Future[TerminateResponse] = {
            logger.info(s"Worekr ${request.id} is terminated")
            if (request.done){

            }
        }
        override def sort(request: SortRequest): Future[SortResponse] = {
            assert (worker_map(request.id).state == 1 || worker_map(request.id).state == 2)
            if (worker_map(request.id).state == 1){
                worker_map.synchronized{
                    worker_map(request.id).state = 2
                }
            }

            if 
        }
    }
}
