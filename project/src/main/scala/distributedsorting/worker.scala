package distributedsorting

object worker extends App {
    def main(args: Array[String]): Unit = {
        val masterIp = args(0).split(":")(0)
        val masterPort = args(0).split(":")(1).toInt
        def parsing(option: String, optionMap: (List[String], List[String]), argList: List[String]): (List[String], List[String]) = {
            if (argList.size == 0) {
                optionMap
            } else {
                match argList.head => {
                    case "-I" => {
                        parsing("-I", optionMap, argList.tail)
                    }
                    case "-O" => {
                        parsing("-O", optionMap, argList.tail)
                    }
                    case x: String => {
                        match option => {
                            case "-I" => {
                                parsing(option, [x :: optionMap._1, optionMap._2], argList.tail)
                            }
                            case "-O" => {
                                parsing(option, [optionMap._1, x :: optionMap._2], argList.tail)
                            }
                            case _ => {
                                print("check option")
                                exit(0)
                            }
                        }
                    }
                }
            }
        }

        val optionMap = parsing("", (List(), List()), args.tail)
        val inputDirectorys = optionMap._1
        val outputDirectorys = optionMap._2

        val workerIp:String = InetAddress.getLocalHost.getHostAddress
        val workerPort:Int = 8888

        val client = new ConnectionClient(masterIp, masterPort, workerIp, workerPort)

        try{
            client.connectRequest

            client.sort

            client.sortRequest

            client.sample

            client.key = ???

            client.sampleRequest

            client.shuffling

            client.shutdown(true)
        }catch{

        }fianlly{
            client.shutdown(false)
        }
    }
}

/*
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
*/
