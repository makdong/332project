package src.main.scala.distributedsorting

class workerInfo(val id:Int, val ip: String, val port: Int){
    var state: Int = 0
    var key: String = null
}