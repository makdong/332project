package distributedsorting

import org.apache.log4j.Logger
import scala.concurrent.{ExecutionContext, Future}
import distributedsorting.connection._
import io.grpc.ServerServiceDefinition
import scala.concurrent.ExecutionContext

class CheckWorkerRunningImpl extends CheckWorkersRunningGrpc.CheckWorkersRunning {
    override def connection(req: ConnectionRequest): Future[ConnectionRespond] = {
        val reply = ConnectionRespond(isWorkerStart = "yesWorker")
        println("worker try connection")
        Future.successful(reply)
    }
}