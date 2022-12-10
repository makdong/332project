package distributedsorting

import scala.io.Source
import java.io._

object FileManager {
    val blockSize = 32000

    def readFileAsBlock(filePath: String): List[String] = {
        def lines = Source.fromFile(filePath).getLines.map(_.concat("\n")).toList
        
        def groupedLines = lines.grouped(blockSize).toList
        
        groupedLines.map(_.mkString)
    }

    def readAsBlock(path: String): String = {
        TypeConverter.entryList2Block(readAsEntries(path))
    }

    def readAsEntries(path: String): List[TypeConverter.Entry] = {
        Source.fromFile(path).getLines.toList.map(TypeConverter.Entry(_))
    }

    def write(path: String, block: String) = {
        val writer = new FileWriter(new File(path))
        writer.write(block)
        writer.close()
    }
}