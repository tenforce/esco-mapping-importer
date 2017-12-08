package parser

import scala.util.{Try, Success, Failure}

trait ColumnsParser {
  // NOTE: We are using a map here because if at some point a column becomes
  // deprecated and we don't want to parse it anymore, we will just have to
  // remove it from the map.
  val columns: Map[Int, ColumnProcessor]

  def parse(values: Seq[String]): Try[Map[String, Any]] = {
    val result: Seq[(Try[Any], Int, String)] = columns
      .toSeq
      .filterNot(x => (x._1 > values.length || values(x._1 - 1).isEmpty) && x._2.optional == true)
      .map { x =>
        if (x._1 <= values.length)
          (x._2.run(values(x._1 - 1)), x._1, x._2.productPrefix)
        else
          (Failure(new Throwable("The column is missing")), x._1, x._2.productPrefix)
      }

    val firstFailure = result.filter(_._1.isFailure).headOption
    firstFailure match {
      case Some(f) =>
        Failure(new Throwable(s"Colmun ${f._2} (${f._3}): ${f._1.failed.get.getMessage}"))
      case None =>
        val values = result.map(x => (x._3, x._1.get)).toMap
        validate(values) match {
          case Some(message) => Failure(new Throwable(s"Validation failed: ${message}"))
          case None => Success(values)
        }
    }
  }

  def validate(values: Map[String, Any]): Option[String]
}

object ColumnsParserTaxonomy extends ColumnsParser {
  val columns: Map[Int, ColumnProcessor] = Map(
    1 -> ConceptStatus,
    2 -> Type,
    3 -> NOCID,
    4 -> ISCO08Ref,
    5 -> PrefLabel,
    6 -> AltLabels,
    7 -> AddLabels,
    8 -> BroaderNOCs,
    9 -> TopConcept,
    10 -> Mappable,
    11 -> RelatedNOCs,
    12 -> Description,
    13 -> DescriptionHTML)

  def validate(values: Map[String, Any]): Option[String] = {
    val topConcept = values("TopConcept").asInstanceOf[String]
    val hasBroaderNOCs = values.get("BroaderNOCs").isDefined
    val hasRelatedNOCs = values.get("RelatedNOCs").isDefined

    if (topConcept == "top" && hasBroaderNOCs)
      return Some(s"${values("NOCID")} is a top concept but it has broader concepts")

    if (values("Type") != "skill" && hasRelatedNOCs)
      return Some(s"${values("NOCID")} is not skill but it has related concepts ")

    if (TaxonomyHeaders.get("Language").isEmpty)
    {
      val message = "No default language set for the document but no explicit language found in %"
      if (values("PrefLabel").asInstanceOf[(String, Set[String], Option[String])]._3.isEmpty)
        return Some(message.replace("%", "PrefLabel"))
      else if (values.get("AltLabels").nonEmpty &&
          values("AltLabels").asInstanceOf[Set[(String, Set[String], Option[String])]].exists(
            x => x._3.isEmpty))
        return Some(message.replace("%", "AltLabels"))
      else if (values.get("AddLabels").nonEmpty &&
          values("AddLabels").asInstanceOf[Set[(String, Set[String], Option[String])]].exists(
            x => x._3.isEmpty))
        return Some(message.replace("%", "AddLabels"))
      else if (values.get("Description").nonEmpty &&
          values("Description").asInstanceOf[Set[(String, Option[String])]].exists(
            x => x._2.isEmpty))
        return Some(message.replace("%", "Description"))
      else if (values.get("DescriptionHTML").nonEmpty &&
          values("DescriptionHTML").asInstanceOf[Set[(String, Option[String])]].exists(
            x => x._2.isEmpty))
        return Some(message.replace("%", "DescriptionHTML"))
    }

    return None
  }
}

object ColumnsParserMapping extends ColumnsParser {
  val columns = Map(
    1 -> NOCID,
    // Column 2 ignored
    3 -> ESCOURI,
    // Column 4 ignored
    5 -> MappingType,
    6 -> MappingStatus)

  def validate(values: Map[String, Any]): Option[String] = None
}
