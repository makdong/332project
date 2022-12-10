package distributedsorting

object worker {

    def makeKeyMedianList(keyString: String) = {
        {
            for (i <- 1 to (keyString.length) / 10) yield {
            keyString substring (10 * (i - 1), (10 * i)) }
        }.toList
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
}
