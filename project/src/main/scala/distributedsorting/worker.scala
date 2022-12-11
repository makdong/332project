package distributedsorting

object worker extends App {
    def main(args: Array[String]): Unit = {
        val masterPort = args(0).toInt
        def parsing(option: String, optionMap: Map[String, Int], argList: List[String]) = {
            
            match argList.head => {
                case "-I" => {
                    parsing("-I", optionMap, argList.tail)
                }
                case "-O" => {
                    parsing("-I", optionMap, argList.tail)
                }
                case x: String => {
                    match option {
                        case "" => {
                            parsing(option, optionMap ++ (option -> x), argList.tail)
                        }
                        case "-I" => {
                            parsing("-I", optionMap, argList.tail)
                        }
                        case "-O" => {
                            parsing("-O", optionMap, argList.tail)
                        }
                    }
                }
            }
        }

        parsing("", )
    }
}