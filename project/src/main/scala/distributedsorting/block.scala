package distributedsorting

case class Block(line: String) {
    def key = line.substring(0, 10)
    def num = line.substring(12, 44)
    def str = line.substring(46, 98)
}
