package distributedsorting

import org.junit.runner.RunWith

import org.scalatestplus.junit.JUnitRunner
import org.scalatest.funsuite.AnyFunSuite


@RunWith(classOf[JUnitRunner])
class Tests extends AnyFunSuite {
    test("Hello World!") {
        assert("Hello World" == "Hello World")
    }

    test("cont check") {
        assert(main.cont == 1)
    }

    test("worker get check") {
        assert(worker.unsortedBlocks.head.key == "~sHd0jDv6X")
        assert(worker.unsortedBlocks.head.num == "00000000000000000000000000000001")
        assert(worker.unsortedBlocks.head.str == "77779999444488885555CCCC777755555555BBBB666644446666")
    }
    test("worker sort check") {
        assert(worker.sortedBlocks.head.key == "AsfAGHM5om")
        assert(worker.sortedBlocks.head.num == "00000000000000000000000000000000")
        assert(worker.sortedBlocks.head.str == "0000222200002222000022220000222200002222000000001111")
    }
}