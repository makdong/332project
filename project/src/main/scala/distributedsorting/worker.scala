package distributedsorting

object worker {
    def sort(entrys: List[T.Entry]): List[T.Entry] = {
        entrys.sortWith((x, y) => x.key < y.key)
    }
}
