package src.main.scala.distributedsorting

import java.io._
import distributedsorting.connection._
import io.grpc._
import scala.util._

import scala.concurrent.{ExecutionContext, Future}

object workerUtil {
    type Block = String
    type Key = String

    def getMedianKeyFromListEntry(entryList: List[TypeConverter.Entry]) : String = {
        val length = entryList.length
        val rand = scala.util.Random
        val idx = (rand.nextInt().abs % length) - 1


        entryList(idx).key
    }

    def getMedianKeyFromKeyList(keyList: List[String]) : String = {
        val length = keyList.length
        val idx = length / 2
        val sortedKeyList = keyList.sorted

        sortedKeyList(idx)
    }

    def key2String(keys : List[String]) : String = {
        keys.mkString
    }

    def string2Key(key: String) : List[String] = {
        key.grouped(10).toList
    }

    def getWorkerOrderUsingKey(keyList: List[String]) : List[Int] = {
        val sortedKeyList = keyList.sorted

        sortedKeyList.map(e => keyList.indexOf(e))
    }

    /*
    get list of entry list, and the range as pivot value.
    return the List of block that the key fits into the range.
     */
    def entryListList2blockList(entryListList: List[List[TypeConverter.Entry]], minRange: String, maxRange: String) : List[Block] = {
        val filteredEntryList = entryListList.flatten filter (
          e => (e.key < maxRange && e.key >= minRange)
          )

        TypeConverter.string2block(TypeConverter.entryList2Block(filteredEntryList))
    }
    def sort(entries: List[TypeConverter.Entry]): List[TypeConverter.Entry] = {
        entries.sortWith((x, y) => x.key < y.key)
    }

    def sortBlock(block: String): String = {
        def entries = TypeConverter.block2EntryList(block)
        def sortedEntry = sort(entries)
        def sortedBlock = TypeConverter.entryList2Block(sortedEntry)
        sortedBlock
    }

    def merge(outputDir: String, listOfBlock: List[List[TypeConverter.Entry]]) = {
        val endValue = TypeConverter.Entry("ENDVALUE!!  00000000000000000000000000000000  0000000000000000000000000000000000000000000000000000\n")
        def findMax(listOfBlock: List[TypeConverter.Entry]) = {
            def line = sort(listOfBlock).head
            def idx = listOfBlock.lastIndexOf(line)
            (idx, line)
        }

        var filePath = outputDir + "/merged0.txt"
        var writer = new FileWriter(new File(filePath))
        def mergeRec(listOfBlock: List[List[TypeConverter.Entry]], count: Int) {
            val listOfHead: List[TypeConverter.Entry] = listOfBlock.map(block => {
                if (block.nonEmpty) block.head
                else endValue
            })
            val max = findMax(listOfHead)

            if (count % 320000 == 0) {
                println("count", count)
                writer.close()
                filePath = outputDir + "/merged" + (count / 320000).toInt.toString + ".txt"
                writer = new FileWriter(new File(filePath))
            }


            if (max._2 != endValue) {
                writer.write(max._2.line)
                mergeRec(listOfBlock.updated(max._1, listOfBlock(max._1).tail), count + 1)
            }
        }
        mergeRec(listOfBlock, 0)
        writer.close()
    }
}
