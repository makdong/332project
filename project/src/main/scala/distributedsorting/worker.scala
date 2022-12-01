package distributedsorting

import distributedsorting.connection.{CheckWorkersRunningGrpc, ConnectionRequest, ConnectionRespond}
import io.grpc.{Server, ServerBuilder}

import scala.concurrent.{ExecutionContext, Future}

object worker {
    val unsortedBlocks: List[Block] = get()

    lazy val sortedBlocks: List[Block] = sort()

    def sort(): List[Block] = {
        unsortedBlocks.sortWith((x, y) => x.key < y.key)
    }

    def get(): List[Block] = {
        val line1 = "~sHd0jDv6X  00000000000000000000000000000001  77779999444488885555CCCC777755555555BBBB666644446666"
        val line2 = "AsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111"

        val block1: Block = Block(line1)
        val block2: Block = Block(line2)
        List(block1, block2)
    }

    def main() = {

        val server = new worker(ExecutionContext.global)
        server.start()
        server.blockUntilShutdown()
    }
}


class worker(executionContext: ExecutionContext) { self =>
    private[this] var server: Server = null

    private def start(): Unit = {
        server = ServerBuilder
          .forPort(8023)
          .addService(CheckWorkersRunningGrpc.bindService(new CheckWorkersRunning, ExecutionContext.global))
          .build()
          .start()

        sys.addShutdownHook {
            System.err.println("*** shutting down gRPC server since JVM is shutting down")
            self.stop()
            System.err.println("*** server shut down")
        }
    }

    private def stop(): Unit = {
        if (server != null) {
            server.shutdown()
        }
    }

    private def blockUntilShutdown(): Unit = {
        if (server != null) {
            server.awaitTermination()
        }
    }

    private class CheckWorkersRunning extends CheckWorkersRunningGrpc.CheckWorkersRunning {
        override def connection (req: ConnectionRequest) = {
            val reply = ConnectionRespond(isWorkerStart = true)
            Future.successful(reply)
        }
    }
}
