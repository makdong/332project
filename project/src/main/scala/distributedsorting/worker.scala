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
        def entries = TypeConverter.BlockToEntries(block)
        def sortedEntry = sort(entries)
        def sortedBlock = TypeConverter.EntriesToBlock(sortedEntry)
        sortedBlock
    }
}
