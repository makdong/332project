package distributedsorting

import org.apache.log4j.Logger
import io.grpc._
import distributedsorting.connection.{CheckWorkersRunningGrpc, ConnectionRequest, ConnectionRespond}
import distributedsorting.distribute.{DistributeBlocksGrpc, DistributeRequest, DistributeRespond}
import distributedsorting.shuffle.{GiveCriteriaGrpc, ShuffleRequest, ShuffleRespond}
import distributedsorting.merge.{MergeBlockGrpc, MergeRequest, MergeRespond}
import scala.concurrent.{ExecutionContext, Future}

object worker {
    def apply() = {
        val channel =
            ManagedChannelBuilder
                .forAddress("192.168.1.44", 9000)
                .build()
    }

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
}
