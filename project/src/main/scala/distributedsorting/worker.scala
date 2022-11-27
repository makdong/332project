package distributedsorting

case class Block(line: String) {
    def key = line.substring(0, 10)
    def num = line.substring(12, 44)
    def str = line.substring(46, 98)
}

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
}
