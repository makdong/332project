package distributedsorting

object worker {
    def sort(entrys: List[TypeConverter.Entry]): List[TypeConverter.Entry] = {
        entrys.sortWith((x, y) => x.key < y.key)
    }

    def sortBlock(block: String): String = {
        def entrys = TypeConverter.BlockToEntrys(block)
        def sortedEntry = sort(entrys)
        def sortedBlock = TypeConverter.EntrysToBlock(sortedEntry)
        sortedBlock
    }
}
