package distributedsorting

object worker {
    type Block = String
    type Key = String

    def getMedianKeyFromListEntry(entryList: List[Entry]) : String = {
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
    def entryListList2blockList(entryListList: List[List[Entry]], minRange: String, maxRange: String) : List[Block] = {
        entryListList.flatten filter (
            e => (e.key < maxRange && e.key >= minRange)
        )
    }
    def sort(entries: List[Entry]): List[Entry] = {
        entries.sortWith((x, y) => x.key < y.key)
    }

    def sortBlock(block: String): String = {
        def entries = TypeConverter.BlockToEntries(block)
        def sortedEntry = sort(entries)
        def sortedBlock = TypeConverter.EntriesToBlock(sortedEntry)
        sortedBlock
    }
}
