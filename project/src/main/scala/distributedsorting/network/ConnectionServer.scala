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
import src.main.scala.distributedsorting.workerInfo

object InvalidStateException extends Exception {}
class ConnectionServer(executionContext: ExecutionContext, port: Int, workerNum: Int) { self=>
    val logger = Logger.getLogger(classOf[ConnectionServer].getName)
    //logger.setLevel(loggerLevel.level)

    var server:Server = null
    val worker_map : Map[Int, workerInfo] = Map()
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

    def check_All(m_state:Int, w_state:Int):Boolean = worker_map.synchronized{
        if (state == m_state && worker_map.size == workerNum && worker_map.forall {case (id, worker) => worker.state == w_state}) true
        else false
    }
    class ConnectionImpl() extends ConnectionGrpc.Connection {
        override def connect(request: ConnectionRequest): Future[ConnectionResponse] = {
            if (state != 0) {
                Future.failed(throw InvalidStateException)
            }
            else {
                logger.info(s"${request.ip}:${request.port}")
                worker_map.synchronized{
                    if(worker_map.size < workerNum) {
                        worker_map(worker_map.size + 1) = new workerInfo(worker_map.size + 1,request.ip, request.port)
                        if(worker_map.size == workerNum){
                            state = 1
                        }
                        Future.successful(new ConnectionResponse(worker_map.size))
                    }
                    else {
                        Future.failed(throw InvalidStateException)
                    }
                }
            }

        }
        override def sample(request: SamplingRequest): Future[SamplingResponse] = {
            assert (worker_map(request.id).state == 2 || worker_map(request.id).state == 3)
            if(worker_map(request.id).state == 2) {
                worker_map.synchronized{
                    worker_map(request.id).state = 3
                    worker_map(request.id).key = request.key
                }
            }
            if(check_All(2,3)){
                state = 3
                logger.info("All Clients finished sending key")
            }
            state match {
                case 3 => {
                    Future.successful(new SamplingResponse(1, workerNum, (worker_map.map{case(id, work_i) => workerSamplingInfo(id = work_i.id, ip = work_i.ip, port = work_i.port, state = work_i.state, key = work_i.key)}).toSeq))
                }
                case 99 => {
                    Future.failed(throw InvalidStateException)
                }
                case _ => {
                    Future.successful(new SamplingResponse(0))
                }
            }
        }

        override def terminate(request: TerminateRequest): Future[TerminateResponse] = {
            logger.info(s"Worker ${request.id} is terminated")
            if (request.done) {
                worker_map.synchronized{
                    worker_map(request.id).state = 4
                    if (check_All(3, 4)) {
                        logger.info("All workers' job is done")
                        val keyIPList: List[(String, String)] = worker_map.map { case (worker_id, worker_info) => (worker_info.ip, worker_info.key) }.toList
                        val keyList: List[String] = keyIPList map (
                          e => e._2
                        )
                        val sortedKeyList = keyList.sorted

                        sortedKeyList map (
                          e => keyIPList.find(keyIP => keyIP._2 == e) match {
                              case None => print("not work!!")
                              case Some (keyIP) => print(keyIP._1 + " ")
                          }
                        )
                        stop
                    }
                }
            } else{
                state = 99
                worker_map.synchronized{
                    val worker = worker_map.remove(request.id)
                    val checkAllTerminate = worker_map.forall{case(_, worker) => worker.state == 4}
                    if(state != 0 && checkAllTerminate) {
                        logger.info("All workers terminated")
                        stop
                    }
                }
            }
            Future.successful(TerminateResponse())
        }
        override def sort(request: SortRequest): Future[SortResponse] = {
            assert (worker_map(request.id).state == 1 || worker_map(request.id).state == 2)
            if (worker_map(request.id).state == 1){
                worker_map.synchronized{
                    worker_map(request.id).state = 2
                }
            }

            if (check_All(1,2)) {
                state = 2
                logger.info("All Clients finished sorting")
            }

            state match {
                case 2 => {
                    Future.successful(new SortResponse(1))
                }
                case 99 => {
                    Future.failed(throw InvalidStateException)
                }
                case _ => {
                    Future.successful(new SortResponse(0))
                }
            }
        }
    }
}
