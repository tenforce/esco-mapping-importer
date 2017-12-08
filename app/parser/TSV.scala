package parser

import scala.io.Source
import parser._

object TSVParser {
  def parseFile(file: java.io.File, encoding: String = "utf-8") = {
    val buffer = Source.fromFile(file, encoding)
    iter(buffer.getLines)
  }

  def unquote(string: String): String = {
    val quotedString = "^\"(.+)\"$".r

    string match {
      case quotedString(content) => content.replace("\"\"", "\"")
      case x => x
    }
  }

  def iter(lines: TraversableOnce[String]): Iterator[Tuple2[Int, Array[String]]] = {
    lines
      .toIterator
      .map("\t".r.split(_))
      .map(_.map(unquote(_)))
      .zipWithIndex
      .map {
        case (x, i) => ((i + 1), x.map(_.trim))
      }
  }
}
