package distributedsorting

object worker {
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
