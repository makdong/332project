package distributedsorting

object TypeConverter {
    case class Entry(line: String) {
        assert(line.length == 99)
        def key = line.substring(0, 10)
        def num = line.substring(12, 44)
        def value = line.substring(46, 98)
        def toLine = line
    }

    def block2EntryList(block: String): List[Entry] = {
        block.split("\n").toList.map(line => Entry(line.concat("\n")))
    }

    def entryList2Block(entryList: List[Entry]): String = {
        entryList.map(_.toLine).mkString
    }
}

