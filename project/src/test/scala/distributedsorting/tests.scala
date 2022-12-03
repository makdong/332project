package distributedsorting

import org.junit.runner.RunWith

import org.scalatestplus.junit.JUnitRunner
import org.scalatest.funsuite.AnyFunSuite


@RunWith(classOf[JUnitRunner])
class Tests extends AnyFunSuite {
    test("Hello World!") {
        assert("Hello World" == "Hello World")
    }

    val line1 = "~sHd0jDv6X  00000000000000000000000000000001  77779999444488885555CCCC777755555555BBBB666644446666"
    val line2 = "AsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111"

    val entry1 = T.Entry(line1)
    val entry2 = T.Entry(line2)

    test("entry parsing check") {
        assert(entry1.key == "~sHd0jDv6X")
        assert(entry1.num == "00000000000000000000000000000001")
        assert(entry1.value == "77779999444488885555CCCC777755555555BBBB666644446666")
        assert(entry2.key == "AsfAGHM5om")
        assert(entry2.num == "00000000000000000000000000000000")
        assert(entry2.value == "0000222200002222000022220000222200002222000000001111")
    }
    
    val entryList: List[T.Entry] = List(entry1, entry2)

    test("worker sort check") {
        val sortedEntryList = worker.sort(entryList)
        assert(sortedEntryList(0).value == "0000222200002222000022220000222200002222000000001111")
        assert(sortedEntryList(1).value == "77779999444488885555CCCC777755555555BBBB666644446666")
    }

    val block = "~sHd0jDv6X  00000000000000000000000000000001  77779999444488885555CCCC777755555555BBBB666644446666\nAsfAGHM5om  00000000000000000000000000000000  0000222200002222000022220000222200002222000000001111"
    test("block test") {
        T.BlockToEntrys(block)
    }
}