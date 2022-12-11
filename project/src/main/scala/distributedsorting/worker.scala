package distributedsorting

object worker extends App {
    def main(args: Array[String]): Unit = {
        val masterPort = args(0).toInt
        def parsing(option: String, optionMap: [List[String], List[String]], argList: List[String]): [List[String], List[String]] = {
            if (argList.size == 0) {
                optionMap
            } else {
                match argList.head => {
                    case "-I" => {
                        parsing("-I", optionMap, argList.tail)
                    }
                    case "-O" => {
                        parsing("-O", optionMap, argList.tail)
                    }
                    case x: String => {
                        match option => {
                            case "-I" => {
                                parsing(option, [x :: optionMap._1, optionMap._2], argList.tail)
                            }
                            case "-O" => {
                                parsing(option, [optionMap._1, x :: optionMap._2], argList.tail)
                            }
                            case _ => {
                                print("check option")
                                exit(0)
                            }
                        }
                    }
                }
            }
        }

        val optionMap = parsing("", [List(), List()], args.tail)
        val inputDirectorys = optionMap._1
        val outputDirectorys = optionMap._2
    }
}