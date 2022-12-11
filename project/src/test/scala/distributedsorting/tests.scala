package src.main.scala.distributedsorting

import org.junit.runner.RunWith

import org.scalatestplus.junit.JUnitRunner
import org.scalatest.funsuite.AnyFunSuite


@RunWith(classOf[JUnitRunner])
class Tests extends AnyFunSuite {
    test("Hello World!") {
        assert("Hello World" == "Hello World")
    }

    val line1 = "~sHd0jDv6X  00000000000000000000000000000001  77779999444488885555CCCC777755555555BBBB666644446666\n"
    val line2 = "AsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111\n"

    val entry1 = TypeConverter.Entry(line1)
    val entry2 = TypeConverter.Entry(line2)

    test("entry parsing check") {
        assert(entry1.key == "~sHd0jDv6X")
        assert(entry1.num == "00000000000000000000000000000001")
        assert(entry1.value == "77779999444488885555CCCC777755555555BBBB666644446666")
        assert(entry2.key == "AsfAGHM5om")
        assert(entry2.num == "00000000000000000000000000000000")
        assert(entry2.value == "0000222200002222000022220000222200002222000000001111")
    }
    
    val entryList: List[TypeConverter.Entry] = List(entry1, entry2)

    test("workerUtil sort check") {
        val sortedEntryList = workerUtil.sort(entryList)
        assert(sortedEntryList(0).value == "0000222200002222000022220000222200002222000000001111")
        assert(sortedEntryList(1).value == "77779999444488885555CCCC777755555555BBBB666644446666")
    }

    val block = "~sHd0jDv6X  00000000000000000000000000000001  77779999444488885555CCCC777755555555BBBB666644446666\nAsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111\n"
    test("block test") {
        val entries = TypeConverter.block2EntryList(block)
        assert(entries == entryList)
        val rebuildedBlock = TypeConverter.entryList2Block(entries)
        assert(rebuildedBlock == block)
    }

    test("block sort test") {
        val sortedBlock = workerUtil.sortBlock(block)
        assert(sortedBlock == "AsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111\n~sHd0jDv6X  00000000000000000000000000000001  77779999444488885555CCCC777755555555BBBB666644446666\n")
    }

    test("sort test") {
        val block1 = FileManager.readFileAsBlock("./sample1.txt").head
        val block2 = FileManager.readFileAsBlock("./sample2.txt").head
        val block3 = FileManager.readFileAsBlock("./sample3.txt").head

        val sorted1 = workerUtil.sortBlock(block1)
        val sorted2 = workerUtil.sortBlock(block2)
        val sorted3 = workerUtil.sortBlock(block3)

        FileManager.write("./sortedSample1.txt", sorted1)
        FileManager.write("./sortedSample2.txt", sorted2)
        FileManager.write("./sortedSample3.txt", sorted3)
    }

    test("merge test") {
        val block1 = FileManager.readFileAsBlock("./sampleInput/unsorted01.txt").head
        val block2 = FileManager.readFileAsBlock("./sampleInput/unsorted02.txt").head
        val block3 = FileManager.readFileAsBlock("./sampleInput/unsorted03.txt").head
        
        println("block1.size", block1.size)
        println("block2.size", block2.size)
        println("block3.size", block3.size)

        val sortedBlock1 = workerUtil.sortBlock(block1)
        val sortedBlock2 = workerUtil.sortBlock(block2)
        val sortedBlock3 = workerUtil.sortBlock(block3)
        
        println("sortedBlock1", sortedBlock1.size)
        println("sortedBlock2", sortedBlock2.size)
        println("sortedBlock3", sortedBlock3.size)

        val listEntry1 = TypeConverter.block2EntryList(sortedBlock1)
        val listEntry2 = TypeConverter.block2EntryList(sortedBlock2)
        val listEntry3 = TypeConverter.block2EntryList(sortedBlock3)
       
        println("listEntry1", listEntry1.size)
        println("listEntry2", listEntry1.size)
        println("listEntry3", listEntry1.size)

        def list = List(listEntry1, listEntry2, listEntry3)
        workerUtil.merge("./sampleOutput", list)
    }
}