package distributedsorting

import scala.io.Source
import java.io._

object FileManager {
    def readAsBlock(path: String): String = {
        TypeConverter.EntrysToBlock(readAsEntrys(path))
    }

    def readAsEntrys(path: String): List[TypeConverter.Entry] = {
        Source.fromFile(path).getLines.toList.map(TypeConverter.Entry(_))
    }

    def write(path: String, block: String) = {
        val writer = new FileWriter(new File(path))
        writer.write(block)
        writer.close()
    }
}