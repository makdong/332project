package distributedsorting

case class Entry(line: String) {
    assert(line.length == 98)

    def key = line.substring(0, 10)

    def num = line.substring(12, 44)

    def value = line.substring(46, 98)

    def toLine = line
}
object TypeConverter {
    def BlockToEntries(block: String): List[Entry] = {
        block.split("\n").toList.map(Entry(_))
    }

    def EntriesToBlock(entries: List[Entry]): String = {
        entries.foldLeft("")(_ + _.toLine + "\n")
    }
}

