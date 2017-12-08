package parser

import scala.util.{Try, Success, Failure}

object Mapping {

  def parseFile(file: java.io.File, encoding: String = "utf-8"):
      (Iterator[(Int, Try[Map[String, Any]])], Iterator[(Int, Try[Map[String, Any]])]) = {
    val lines = TSVParser.parseFile(file, encoding)
    val (headerLines, content) = lines.span(x => x._2.length == 2)

    MappingHeaders.parse(headerLines)

    content
      .drop(1) // Drop the header line from the TSV
      .map(x => (x._1, ColumnsParserMapping.parse(x._2)))
      .span(_._2.isSuccess)
  }

}
