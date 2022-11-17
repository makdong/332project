import common._
import file._

object Main extends App {
    val blockSize = 100
    println("generate testbench with block size : " + blockSize)
    val unsortedBlock = File.readBlockFromFile("./origin.txt")
    val sortedBlock = unsortedBlock.sortWith(_._1 < _._1)
    File.writeBlockToFile("./output.txt", sortedBlock)
}