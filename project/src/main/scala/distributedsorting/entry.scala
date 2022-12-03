package distributedsorting

object TypeConverter {
    case class Entry(line: String) {
        assert(line.length == 98)
        def key = line.substring(0, 10)
        def num = line.substring(12, 44)
        def value = line.substring(46, 98)
        def toLine = line
    }

    def BlockToEntrys(block: String): List[Entry] = {
        block.split("\n").toList.map(Entry(_))
    }

    def EntrysToBlock(entrys: List[Entry]): String = {
        entrys.foldLeft("")(_ + _.toLine + "\n")
    }
}

