package distributedsorting

object worker {
    def sort(entrys: List[TypeConverter.Entry]): List[TypeConverter.Entry] = {
        entrys.sortWith((x, y) => x.key < y.key)
    }
}
