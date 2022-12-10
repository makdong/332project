package distributedsorting

import distributedsorting.connection._
import io.grpc._

import scala.concurrent.{ExecutionContext, Future}

object worker {
    type Block = String
    type Key = String

    def getMedianKeyFromListEntry(entryList: List[TypeConverter.Entry]) : String = {
        val length = entryList.length
        val idx = length / 2


        entryList(idx).key
    }

    def getMedianKeyFromKeyList(keyList: List[String]) : String = {
        val length = keyList.length
        val idx = length / 2
        val sortedKeyList = keyList.sorted

        sortedKeyList(idx)
    }

    def key2String(keys : List[String]) : String = {
        keys.mkString
    }

    def string2Key(key: String) : List[String] = {
        key.grouped(10).toList
    }

    def getWorkerOrderUsingKey(keyList: List[String]) : List[Int] = {
        val sortedKeyList = keyList.sorted

        sortedKeyList.map(e => keyList.indexOf(e))
    }

    /*
    get list of entry list, and the range as pivot value.
    return the List of block that the key fits into the range.
     */
    def entryListList2blockList(entryListList: List[List[TypeConverter.Entry]], minRange: String, maxRange: String) : List[Block] = {
        val filteredEntryList = entryListList.flatten filter (
            e => (e.key < maxRange && e.key >= minRange)
        )

        TypeConverter.string2block(TypeConverter.entryList2Block(filteredEntryList))
    }
    def sort(entries: List[TypeConverter.Entry]): List[TypeConverter.Entry] = {
        entries.sortWith((x, y) => x.key < y.key)
    }

    def sortBlock(block: String): String = {
        def entries = TypeConverter.block2EntryList(block)
        def sortedEntry = sort(entries)
        def sortedBlock = TypeConverter.entryList2Block(sortedEntry)
        sortedBlock
    }

    def merge(listOfBlock: List[List[Int]]) = {
        val endValue = -1
        def findMax(listOfBlock: List[Int]) = {
            def line = listOfBlock.max
            def idx = listOfBlock.lastIndexOf(line)
            (idx, line)
        }

        def mergeRec(listOfBlock: List[List[Int]]) {
            val listOfHead = listOfBlock.map(block => {
                if (block.nonEmpty) block.head
                else endValue
            })
            val max = findMax(listOfHead)

            if (max._2 != endValue) {
                println(max._2)
                mergeRec(listOfBlock.updated(max._1, listOfBlock(max._1).tail))
            }
        }
        mergeRec(listOfBlock)
    }

    def main() = {

        //val server = new worker(ExecutionContext.global)
        //server.start()
        //server.blockUntilShutdown()
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