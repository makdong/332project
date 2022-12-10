package src.main.scala.distributedsorting

import scala.concurrent.{ExecutionContext, Future, Promise, Await}
import scala.concurrent.ExecutionContext.Implicits.global
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
    class ConnectionImpl() extends ConnectionGrpc.Connection {
        override def connect(request: ConnectionRequest): Future[ConnectionResponse] = {
            //logger.info(s"${request.ip}:${request.port}")
        }

        override def sample(request: SamplingRequest): Future[SamplingResponse] {

        }

        override def terminate(request: TerminateRequest): Future[TerminateResponse] = {
            //logger.info(s"Worekr ${request.id} is terminated")
        }
    }
}