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
        val block4 = FileManager.readFileAsBlock("./sampleInput/unsorted04.txt").head
        val block5 = FileManager.readFileAsBlock("./sampleInput/unsorted05.txt").head
        val block6 = FileManager.readFileAsBlock("./sampleInput/unsorted06.txt").head
        val block7 = FileManager.readFileAsBlock("./sampleInput/unsorted07.txt").head
        val block8 = FileManager.readFileAsBlock("./sampleInput/unsorted08.txt").head
        val block9 = FileManager.readFileAsBlock("./sampleInput/unsorted09.txt").head
        val block10 = FileManager.readFileAsBlock("./sampleInput/unsorted10.txt").head
        val block11 = FileManager.readFileAsBlock("./sampleInput/unsorted11.txt").head
        val block12 = FileManager.readFileAsBlock("./sampleInput/unsorted12.txt").head
        val block13 = FileManager.readFileAsBlock("./sampleInput/unsorted13.txt").head
        val block14 = FileManager.readFileAsBlock("./sampleInput/unsorted14.txt").head
        val block15 = FileManager.readFileAsBlock("./sampleInput/unsorted15.txt").head
        val block16 = FileManager.readFileAsBlock("./sampleInput/unsorted16.txt").head
        
        val mbBlockList = TypeConverter.string2block(block1)
        mbBlockList.zipWithIndex.foreach(block => {
            FileManager.write("./mbSample/" + block._2.toString + ".txt", block._1)
        })

        println("block1.size", block1.size)
        println("block2.size", block2.size)
        println("block3.size", block3.size)

        val sortedBlock1 = workerUtil.sortBlock(block1)
        val sortedBlock2 = workerUtil.sortBlock(block2)
        val sortedBlock3 = workerUtil.sortBlock(block3)
        val sortedBlock4 = workerUtil.sortBlock(block4)
        val sortedBlock5 = workerUtil.sortBlock(block5)
        val sortedBlock6 = workerUtil.sortBlock(block6)
        val sortedBlock7 = workerUtil.sortBlock(block7)
        val sortedBlock8 = workerUtil.sortBlock(block8)
        val sortedBlock9 = workerUtil.sortBlock(block9)
        val sortedBlock10 = workerUtil.sortBlock(block10)
        val sortedBlock11 = workerUtil.sortBlock(block11)
        val sortedBlock12 = workerUtil.sortBlock(block12)
        val sortedBlock13 = workerUtil.sortBlock(block13)
        val sortedBlock14 = workerUtil.sortBlock(block14)
        val sortedBlock15 = workerUtil.sortBlock(block15)
        val sortedBlock16 = workerUtil.sortBlock(block16)

        
        println("sortedBlock1", sortedBlock1.size)
        println("sortedBlock2", sortedBlock2.size)
        println("sortedBlock3", sortedBlock3.size)

        val listEntry1 = TypeConverter.block2EntryList(sortedBlock1)
        val listEntry2 = TypeConverter.block2EntryList(sortedBlock2)
        val listEntry3 = TypeConverter.block2EntryList(sortedBlock3)
        val listEntry4 = TypeConverter.block2EntryList(sortedBlock4)
        val listEntry5 = TypeConverter.block2EntryList(sortedBlock5)
        val listEntry6 = TypeConverter.block2EntryList(sortedBlock6)
        val listEntry7 = TypeConverter.block2EntryList(sortedBlock7)
        val listEntry8 = TypeConverter.block2EntryList(sortedBlock8)
        val listEntry9 = TypeConverter.block2EntryList(sortedBlock9)
        val listEntry10 = TypeConverter.block2EntryList(sortedBlock10)
        val listEntry11 = TypeConverter.block2EntryList(sortedBlock11)
        val listEntry12 = TypeConverter.block2EntryList(sortedBlock12)
        val listEntry13 = TypeConverter.block2EntryList(sortedBlock13)
        val listEntry14 = TypeConverter.block2EntryList(sortedBlock14)
        val listEntry15 = TypeConverter.block2EntryList(sortedBlock15)
        val listEntry16 = TypeConverter.block2EntryList(sortedBlock16)
       
        println("randomKey", workerUtil.getMedianKeyFromListEntry(listEntry1))
        println("listEntry1", listEntry1.size)
        println("listEntry2", listEntry1.size)
        println("listEntry3", listEntry1.size)

        def list = List(
            listEntry1, 
            listEntry2, 
            listEntry3,
            listEntry4,
            listEntry5,
            listEntry6,
            listEntry7,
            listEntry8,
            listEntry9,
            listEntry10,
            listEntry11,
            listEntry12,
            listEntry13,
            listEntry12,
            listEntry14,
            listEntry15,
            listEntry16,
            )
        workerUtil.merge("./sampleOutput", list)
    }
}