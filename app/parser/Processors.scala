package parser

import com.netaporter.uri.Uri
import java.util.Locale
import scala.util.{Try, Success, Failure}
import scala.util.matching.Regex

trait ColumnProcessor extends Product {

  /**
   * A ColumnProcessor will "process" the file's content. It doesn't handle the
   * columns themselves, it is ran against a single value.
   *
   * The processor's runner is common to all the processors. You may override
   * it when writing your processor but you should just use the default
   * behavior.
   *
   * The default behavior:
   *
   *  - match the value against a regular expression stored in the variable
   *    "regex" of the object;
   *  - transform conveniently the groups found in the regex result to
   *    Option[String] (no null values to handle);
   *  - call the "parse" method of the object and expands the groups found by
   *    the regex as parameters to the call (groups: Option[String]*);
   *  - automatically returns a default message ("Can't parse: ...") if the
   *    regex didn't match at all;
   *  - transform your result to a Success(result) (if it's not already a Try type).
   */

  val iscoRegex = "ISCO-(\\d{4})".r
  val langRegex = "[a-z][a-z]".r
  val genderRegex = "(?:f|m|n|sf|sm)".r
  val gendersRegex = s"${genderRegex}(?:,\\s*${genderRegex})*".r
  val labelRegex = s"(?:\\[(${langRegex})\\])?\\s*(.+?)\\s*(?:\\((${gendersRegex})\\))?".r
  val NOCRegex = raw"NOC-\w+".r
  val descriptionRegex = "(?:\\[([a-z]{2})\\])?\\s*(.+)".r
  val descriptionsRegex = s"((${descriptionRegex})(\\s*@@\\s*${descriptionRegex})*)?".r

  def parseLabel(groups: Option[String]*): Try[(String, Set[String], Option[String])] = {
    val lang = groups(0)
    val text = groups(1).get
    val genders: Set[String] = groups(2) match {
        case Some(x) => "\\s*,\\s*".r.split(groups(2).get).toSet
        case None => Set()
      }
    if (lang.nonEmpty && Locale.getISOLanguages.find(_.==(lang.get)).isEmpty)
      Failure(new Throwable(s"Invalid language: ${lang.get}"))
    else
      Success((text, genders, lang))
  }

  def parseDescripton(groups: Option[String]*): Try[(String, Option[String])] = {
    val lang = groups(0)
    val text = groups(1).get
    if (lang.nonEmpty && Locale.getISOLanguages.find(_.==(lang.get)).isEmpty)
      Failure(new Throwable(s"Invalid language: ${lang.get}"))
    else
      Success((text, lang))
  }

  var regex: Regex
  val optional: Boolean = false
  def process(groups: Option[String]*): Any

  def validate(result: Any): Try[Any] = {
    result match {
      case Success(x) => Success(x)
      case Failure(e) => Failure(e)
      case x => Success(x)
    }
  }

  def run(string: String): Try[Any] = {
    string match {
      case regex(rest @ _*) =>
        val result = process(rest.map(Option(_)):_*)
        validate(result)
      case _ => Failure(new Throwable(s"Can't parse: ${string}"))
    }
  }
}

trait MultiValueColumnProcessor extends ColumnProcessor {
  var regexSeparator: Regex

  // TODO: not clean, it gets rid of this required method
  override def process(groups: Option[String]*): Any = {}

  def process(found: TraversableOnce[Seq[Option[String]]]): Any

  override def run(string: String): Try[Any] = {
    val items: Array[Try[Seq[Option[String]]]] = regexSeparator.split(string)
      .map({
        case regex(groups @ _*) => Success(groups.map(Option(_)).toSeq)
        case x => Failure(new Throwable(s"Can't parse: ${x}"))
      })
    val (success, failures) = items.partition(_.isSuccess)
    if (failures.nonEmpty)
      failures(0)
    else
    {
      val result = process(success.map(x => x.get))
      validate(result)
    }
  }
}

// NOTE: since we are going to use the exact same column processor for AltLabels and AddLabels,
// let's make a trait
trait MultipleLabels extends MultiValueColumnProcessor {
  var regex = labelRegex
  var regexSeparator = raw"\s*@@\s*".r

  def process(found: TraversableOnce[Seq[Option[String]]]): Try[Set[(String, Set[String], Option[String])]] = {
    val labels = found.map(x => parseLabel(x:_*)).toSeq
    labels.filter(_.isFailure).headOption match {
      case Some(x) => Failure(x.failed.get)
      case None => Success(labels.map(_.get).toSet)
    }
  }
}

// NOTE: same format for BroaderNOCs and RelatedNOCs
trait MultipleNOCs extends MultiValueColumnProcessor {
  var regex = s"(${NOCRegex})".r
  var regexSeparator = raw"\s*@@\s*".r
  override val optional = true

  def process(found: TraversableOnce[Seq[Option[String]]]): Try[Set[String]] = {
    val ids: Seq[String] = found.toSeq.map(x => x(0).get)
    val setOfIds = ids.toSet
    if (ids.length == setOfIds.toSeq.length)
      Success(setOfIds)
    else
      Failure(new Throwable("Duplicates found"))
  }
}

// NOTE: same format for Description and DescriptionHTML
trait MultipleDescriptions extends MultiValueColumnProcessor {
  var regex = descriptionRegex
  var regexSeparator = raw"\s*@@\s*".r

  def process(found: TraversableOnce[Seq[Option[String]]]): Try[Set[(String, Option[String])]] = {
    val descriptions = found.map(x => parseDescripton(x:_*)).toSeq
    descriptions.filter(_.isFailure).headOption match {
      case Some(x) => Failure(x.failed.get)
      case None => Success(descriptions.map(_.get).toSet)
    }
  }
}

case object ConceptStatus extends ColumnProcessor {
  var regex: Regex = "(?i)(active|inactive)".r

  def process(groups: Option[String]*): String = groups(0).get.toLowerCase
}

case object MappingStatus extends ColumnProcessor {
  var regex: Regex = "(?i)(todo|approved|rejected|removed)".r
  override val optional: Boolean = true

  def process(groups: Option[String]*): String = groups(0).get.toLowerCase
}

case object Type extends ColumnProcessor {
  var regex: Regex = "(?i)(occupation|skill)".r

  def process(groups: Option[String]*): String = groups(0).get.toLowerCase
}

case object ISCO08Ref extends ColumnProcessor {
  var regex = s"${iscoRegex}".r
  override val optional: Boolean = true

  def process(groups: Option[String]*): String = {
    groups(0).get
  }
}

case object NOCID extends ColumnProcessor {
  var regex = s"(${NOCRegex})".r

  def process(groups: Option[String]*): String = {
    groups(0).get
  }
}

case object ESCOURI extends ColumnProcessor {
  var regex = "(.+)".r

  def process(groups: Option[String]*): Try[Uri] = Try(Uri.parse(groups(0).get))
}

case object PrefLabel extends ColumnProcessor {
  var regex = labelRegex

  def process(groups: Option[String]*): Try[(String, Set[String], Option[String])] = {
    parseLabel(groups:_*)
  }
}

case object AltLabels extends MultipleLabels {
  override val optional = true
}

case object AddLabels extends MultipleLabels {
  override val optional = true
}

case object BroaderNOCs extends MultipleNOCs {
  override val optional = true
}

case object RelatedNOCs extends MultipleNOCs {
  override val optional = true
}

case object TopConcept extends ColumnProcessor {
  var regex = s"(?i)(top|group|member)".r

  def process(groups: Option[String]*): String = groups(0).get.toLowerCase
}

case object Mappable extends ColumnProcessor {
  var regex = s"(?i)(y|n)".r

  def process(groups: Option[String]*): Boolean = groups(0).get.toLowerCase == "y"
}

case object Description extends MultipleDescriptions {
  override val optional = true
}

case object DescriptionHTML extends MultipleDescriptions {
  override val optional = true
}

case object MappingType extends ColumnProcessor {
  var regex = "(exact|broad|narrow)".r

  def process(groups: Option[String]*): String = groups(0).get.toLowerCase
}
