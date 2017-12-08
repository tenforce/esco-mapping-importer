package parser

import com.netaporter.uri.dsl._
import java.util.Locale
import config.Config

trait Headers {
  var headers: Map[String, String] = Map()

  def parse(lines: TraversableOnce[(Int, Array[String])]) = {
    headers = lines
      .map(x => x._2)
      .map(x => (":$".r.replaceAllIn(x(0), ""), x(1)))
      .toMap
  }

  def get(key: String): Option[String] = headers.get(key)

  def apply(key: String): String = headers(key)
}

object TaxonomyHeaders extends Headers {
  val validURIChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
  val requiredHeaders = Seq(
    "Classification identifier",
    "Classification version")
  val requiresValidURIChars = Seq(
    "Classification identifier",
    "Classification version")

  implicit class CharExtension(x: Char) {
    def isURICompatibleChar: Boolean = validURIChars.exists(_.==(x))
  }

  def validate: Option[String] = {
    for (name <- requiredHeaders)
      if (headers.get(name).isEmpty)
        return Some(s"Missing header: $name")

    for (name <- requiresValidURIChars)
      if (!headers(name).forall(_.isURICompatibleChar))
        return Some(s"$name has invalid characters (only ${validURIChars} are allowed)")

    if (headers.get("Language").nonEmpty &&
        Locale.getISOLanguages.find(_.==(headers("Language"))).isEmpty)
      return Some(s"Invalid language (${headers("Language")}), not a valid ISO-639 language")

    return None
  }
}

object MappingHeaders extends Headers {
  def validateNOCType: Boolean = {
    val regex = "(?i)(occupation|skill|qualification)".r
    nocType match {
      case regex(_) => true
      case _ => false
    }
  }

  def validate: Option[String] = {
    if (headers.get("Mapping version").isEmpty)
      return Some(s"Missing header: Mapping version")

    if (headers.get("From URI").isEmpty)
    {
      if (headers.get("From ID").isEmpty || headers.get("From version").isEmpty ||
          headers.get("From type").isEmpty)
        return Some(Seq(
          "Missing header: either `From URI` is provided or",
          "`From ID` with `From version` and `From type` must be specified").mkString(" "))
    }

    if (headers.get("To URI").isEmpty)
    {
      if (headers.get("To ID").isEmpty || headers.get("To version").isEmpty ||
          headers.get("To type").isEmpty)
        return Some(Seq(
          "Missing header: either `To URI` is provided or",
          "`To ID` with `To version` and `To type` must be specified").mkString(" "))
    }

    if (headers.get("From URI").isEmpty && headers.get("To URI").isEmpty)
      return Some("You must specify at least a ESCO taxonomy in `From` or `To`")

    if (headers.get("From URI").nonEmpty && headers.get("To URI").nonEmpty)
      return Some("You must specify at least a ROME taxonomy in `From` or `To`")

    if (validateNOCType == false)
      return Some(Seq(
        "Invalid NOC type value, must be one of the following: ",
        "occupation, skill, qualification").mkString(" "))

    return None
  }

  def escoURI = headers.get("From URI") match {
    case Some(x) => x
    case None => headers.get("To URI").get
  }

  def nocIdentifier = headers.get("From ID") match {
    case Some(x) => x
    case None => headers("To ID")
  }

  def nocVersion = headers.get("From version") match {
    case Some(x) => x
    case None => headers("To version")
  }

  def nocType = headers.get("From type") match {
    case Some(x) => x.toLowerCase
    case None => headers("To type").toLowerCase
  }

  private def toURI(string: String): String = string.replace(" ", "-").toLowerCase

  def nocURI: String = "http://data.europa.eu/esco/concept-scheme" /
    toURI(nocIdentifier) / toURI(nocVersion) / {
      nocType match {
        case "occupation" => "occupations"
        case "skill" => "skills"
        case "qualification" => "qualifications"
      }
    }

  def isToEsco: Boolean = headers.get("To URI") match {
    case Some(_) => true
    case None => false
  }

  def isFromEsco: Boolean = !isToEsco
}
