package distributedsorting

object T {
    case class Entry(line: String) {
        assert(line.length == 98)
        def key = line.substring(0, 10)
        def num = line.substring(12, 44)
        def value = line.substring(46, 98)
    }

    def BlockToEntrys(block: String) {
        block.split("\n").toList.map(Entry(_))
    }
}

