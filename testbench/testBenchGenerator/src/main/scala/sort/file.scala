package file

import scala.io.Source
import java.io._

object File {
    type Line = (String, String)

    type Block = List[Line]

    def readBlockFromFile(filePath: String): Block = {
        try {
            val linesFromFile = Source.fromFile(filePath).getLines.toList
            linesFromFile.map(x => (x.substring(0, 9), x))
        } catch {
            case ex: Exception => println(ex)
            Nil
        }
    }

    def writeBlockToFile(filePath: String, block: Block) {
        val fileWriter = new FileWriter(new File(filePath))
        block.foreach(x => {
            fileWriter.write(x._2)
            fileWriter.write("\n")
        })
        fileWriter.close()
    }
}