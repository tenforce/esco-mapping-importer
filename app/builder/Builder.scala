package builder

import io.jvm.uuid.UUID
import com.netaporter.uri.dsl._
import org.scala_tools.time.Imports._
import scala.collection.mutable
import config.Config
import parser.{Headers, TaxonomyHeaders, MappingHeaders}

object MapExtensions {
  implicit class MapPredicates(m: Map[String, Any]) {

    def getAs[T](key: String): Option[T] = {
      m.get(key) match {
        case Some(x) => Some(x.asInstanceOf[T])
        case None => None
      }
    }

    def getAsOrElse[T](key: String, default: T): T = {
      m.get(key) match {
        case Some(x) => x.asInstanceOf[T]
        case None => default
      }
    }

  }
}

trait Builder {
  val headers: Headers
  val prefixes = Map(
    "rdf" -> "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc" -> "http://purl.org/dc/elements/1.1/",
    "mu" -> "http://mu.semte.ch/vocabularies/core/",
    "ext" -> "http://mu.semte.ch/vocabularies/ext/",
    "doap" -> "http://usefulinc.com/ns/doap#",
    "w3vocab" -> "https://www.w3.org/1999/xhtml/vocab#",
    "auth" -> "http://mu.semte.ch/vocabularies/authorization/",
    "dcterms" -> "http://purl.org/dc/terms/",
    "foaf" -> "http://xmlns.com/foaf/0.1/",
    "skos" -> "http://www.w3.org/2004/02/skos/core#",
    "skosxl" -> "http://www.w3.org/2008/05/skos-xl#",
    "skosthes" -> "http://purl.org/iso25964/skos-thes#",
    "esco" -> "http://data.europa.eu/esco/model#",
    "mp" -> "http://sem.tenforce.com/vocabularies/mapping-platform/",
    "hier" -> "http://mu.semte.ch/vocabularies/hierarchy/",
    "owl" -> "http://www.w3.org/2002/07/owl#")

  var mimeType: String

  def transform(it: TraversableOnce[Map[String, Any]]): TraversableOnce[Any]

  def generateRandomURI(`type`: String): (String, UUID) = {
    val uuid = UUID.random
    (s"http://data.europa.eu/esco" / `type` / uuid.toString, uuid)
  }

  lazy val (graphURI, graphUUID) = generateRandomURI("graph")
}

trait BuilderTaxonomy extends Builder {
  val headers = TaxonomyHeaders

  // NOTE: being lazy here is not really helpful. I believe the class and
  // objects are already lazy in Scala and Java (I think I read that
  // somewhere...). That's why it works event without being lazy. But I think
  // it's sometimes better when it's explicit.
  lazy val now: String = DateTime.now.toString
  // NOTE: the only reason why I'm making classes from this trait is because
  // this mapping (NOCID -> URI) need to be recreated at every call to the endpoint.
  lazy val URIMap: mutable.Map[String, (String, UUID)] = mutable.Map()
  lazy val occupationSchemePrefLabel =
    s"${TaxonomyHeaders("Classification identifier")} Occupations"
  lazy val skillSchemePrefLabel =
    s"${TaxonomyHeaders("Classification identifier")} Skills"
  lazy val qualificationSchemePrefLabel =
    s"${TaxonomyHeaders("Classification identifier")} Qualifications"
  lazy val (occupationScheme, occupationUUID) =
    generateURI("concept-scheme", "occupations")
  lazy val (skillScheme, skillUUID) =
    generateURI("concept-scheme", "skills")
  lazy val (qualificationScheme, qualificationUUID) =
    generateURI("concept-scheme", "qualifications")
  lazy val graph = Config.applicationSPARQL.graph /
    TaxonomyHeaders("Classification identifier") / TaxonomyHeaders("Classification version")

  def toURI(string: String): String = string.replace(" ", "-").toLowerCase

  def getLabelRole(short: String): String = {
    short match {
      case "f" => "http://data.europa.eu/esco/LabelRole#iC.genderFemale"
      case "m" => "http://data.europa.eu/esco/LabelRole#iC.genderMale"
      case "n" => "http://data.europa.eu/esco/LabelRole#iC.genderNeutral"
      case "sf" => "http://data.europa.eu/esco/LabelRole#iC.genderStandardFemale"
      case "sm" => "http://data.europa.eu/esco/LabelRole#iC.genderStandardMale"
    }
  }

  override def generateRandomURI(`type`: String): (String, UUID) = {
    val conceptCase = "(occupation|skill|qualification)".r
    val uuid = UUID.random
    `type` match {
      case conceptCase(concept) =>
        (s"http://data.europa.eu/esco" / s"${toURI(TaxonomyHeaders("Classification identifier"))}-${concept}" / uuid.toString, uuid)
      case "concept-scheme" =>
        (s"http://data.europa.eu/esco/concept-scheme" /
          toURI(TaxonomyHeaders("Classification identifier")) /
          toURI(TaxonomyHeaders("Classification version")),
        uuid)
      case x =>
        (s"http://data.europa.eu/esco" / x / uuid.toString, uuid)
    }
  }

  def generateURI(`type`: String, id: String): (String, UUID) = {
    val classification: String = TaxonomyHeaders.get("Classification identifier").get
    val version: String = TaxonomyHeaders.get("Classification version").get
    val name = s"${classification}__${version}__${`type`}__${id}"
    URIMap.get(name) match {
      case Some(x) => x
      case None =>
        val (uri, uuid) = generateRandomURI(`type`)
        `type` match {
          case "concept-scheme" => URIMap(name) = (uri / id, uuid)
          case _ => URIMap(name) = (uri, uuid)
        }
        URIMap(name)
    }
  }
}

trait BuilderMapping extends Builder {
  val headers = MappingHeaders

  lazy val graph = Config.applicationSPARQL.graph /
    MappingHeaders.nocIdentifier / MappingHeaders.nocVersion /
    "mapping" / MappingHeaders("Mapping version")
}
