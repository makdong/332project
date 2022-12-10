package distributedsorting

object TypeConverter {
    case class Entry(line: String) {
        assert(line.length == 99)
        def key = line.substring(0, 10)
        def num = line.substring(12, 44)
        def value = line.substring(46, 98)
        def toLine = line
    }

    def BlockToEntries(block: String): List[Entry] = {
        block.split("\n").toList.map(line => Entry(line.concat("\n")))
    }

    def EntriesToBlock(entries: List[Entry]): String = {
        entries.map(_.toLine).mkString
    }
}

