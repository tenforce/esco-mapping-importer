package builder

import com.netaporter.uri.Uri
import play.api.Logger
import play.api.libs.iteratee.Iteratee
import play.api.libs.json._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import config.Config

class NOCIDNotFound(val message: String) extends Exception

trait Turtle extends Builder {
  val documentID: String
  var mimeType = "text/turtle"

  def flattenNonEmpty(sep: String, values: String*): String = {
    values.filter(_.nonEmpty).mkString(sep)
  }

  def generatePrefixes: String = {
    prefixes.toSeq.sorted.map(x => s"@prefix ${x._1}: <${x._2}> .").mkString("\n")
  }
}

class TurtleTaxonomy(val documentID: String) extends BuilderTaxonomy with Turtle {
  import MapExtensions._
  import sparql.SPARQLHelpers.escapeURI

  def escapeString(string: String) = sparql.SPARQLHelpers.escapeString(string)

  def escapeString(string: String, lang: Option[String]): String = {
    "\"" + string.flatMap(_ match {
      case '\"' => "\\\""
      case '\\' => "\\\\"
      case '\u000a' => "\\n"
      case '\u000d' => "\\r"
      case c => c.toString
    }) + "\"" + {
      lang match {
        case Some(x) => s"@${x}"
        case None => s"@${headers("Language")}"
      }
    }
  }

  def generateMetadata: String = {
    s"""${generatePrefixes}
    |
    |<$graphURI>
    |  a esco:Graph ;
    |  mu:uuid "$graphUUID" ;
    |  esco:graph <$graph> ;
    |  esco:documentID "$documentID" ;
    |  esco:databaseURL <${Config.applicationSPARQL.crudEndpoint}> ;
    |  esco:NOCClassification ${escapeString(headers("Classification identifier"))} ;
    |  esco:NOCVersion ${escapeString(headers("Classification version"))} ;
    |  esco:status esco:NotValidated .
    |""".stripMargin
  }

  def transform(it: TraversableOnce[Map[String, Any]]): TraversableOnce[String] = {
    Iterator(this.generateHeader) ++
      it.filter(x => x.get("ConceptStatus").get == "active").map(this.generateConcept)
  }

  def generateHeader: String = {
    s"""${generatePrefixes}
    |
    |<${occupationScheme}>
    |  a skos:ConceptScheme, esco:ConceptScheme ;
    |  mu:uuid "${occupationUUID}" ;
    |  skos:prefLabel ${escapeString(occupationSchemePrefLabel)} ;
    |  dcterms:description ${escapeString(occupationSchemePrefLabel)} ;
    |  dcterms:locale ${escapeString(headers("Language"))} ;
    |  owl:versionInfo ${escapeString(headers("Classification version"))} ;
    |  esco:NOCConceptType "occupation" .
    |
    |<${skillScheme}>
    |  a skos:ConceptScheme, esco:ConceptScheme ;
    |  mu:uuid "${skillUUID}" ;
    |  skos:prefLabel ${escapeString(skillSchemePrefLabel)} ;
    |  dcterms:description ${escapeString(skillSchemePrefLabel)} ;
    |  dcterms:locale ${escapeString(headers("Language"))} ;
    |  owl:versionInfo ${escapeString(headers("Classification version"))} ;
    |  esco:NOCConceptType "skill" .
    |
    |<${qualificationScheme}>
    |  a skos:ConceptScheme, esco:ConceptScheme ;
    |  mu:uuid "${qualificationUUID}" ;
    |  skos:prefLabel ${escapeString(qualificationSchemePrefLabel)} ;
    |  dcterms:description ${escapeString(qualificationSchemePrefLabel)} ;
    |  dcterms:locale ${escapeString(headers("Language"))} ;
    |  owl:versionInfo ${escapeString(headers("Classification version"))} ;
    |  esco:NOCConceptType "qualification" .
    |
    |""".stripMargin
  }

  def generateConcept(values: Map[String, Any]): String = {
    Logger.debug(s"Generating concept from values: ${values}")

    val (conceptURI, conceptUUID) =
      generateURI(values.getAs[String]("Type").get, values.getAs[String]("NOCID").get)
    val prefLabel = values.getAs[(String, Set[String], Option[String])]("PrefLabel").get
    val altLabels =
      values.getAsOrElse[Set[(String, Set[String], Option[String])]]("AltLabels", Set())
    val relatedNOCs = values.getAsOrElse[Set[String]]("RelatedNOCs", Set())
    val conceptScheme = values.getAs[String]("Type").get match {
      case "occupation" => occupationScheme
      case "skill" => skillScheme
      case "qualification" => qualificationScheme
    }
    val topConcept = values.getAs[String]("TopConcept").get

    flattenNonEmpty(
      "\n\n",
      flattenNonEmpty(
        "\n",
        s"""<${conceptURI}>
        |  a skos:Concept, esco:NOCConcept ;
        |  mu:uuid "${conceptUUID}" ;
        |  dcterms:created ${escapeString(now)}^^xsd:dateTime ;
        |  dcterms:modified ${escapeString(now)}^^xsd::dateTime ;
        |  skos:inScheme <${conceptScheme}> ;
        |  esco:referenceLanguage ${escapeString(headers("Language"))} ;
        |  esco:editorialStatus "proposed" ;""".stripMargin,
        {
          values.getAs[String]("ISCO08Ref") match {
            case Some(x) =>
              s"  esco:memberOfISCOGroup <http://data.europa.eu/esco/isco2008/Concept/C$x> ;"
            case None => ""
          }
        },
        {
          values.getAsOrElse[Set[(String, Option[String])]]("Description", Set())
            .map(x => s"""  dcterms:description ${escapeString(x._1, x._2)} ;""")
            .mkString("\n")
        },
        {
          values.getAsOrElse[Set[(String, Option[String])]]("DescriptionHTML", Set())
            .map(x => s"""  dcterms:descriptionHTML ${escapeString(x._1, x._2)} ;""")
            .mkString("\n")
        },
        {
          values.getAsOrElse[Set[String]]("BroaderNOCs", Set())
            .map(x => s"""  skos:broader <${generateURI("occupation", x)._1}> ;""")
            .mkString("\n")
        },
        {
          if (topConcept == "top")
            s"  skos:topConceptOf <${conceptScheme}> ;"
          else
            ""
        },
        {
          if (values.getAs[Boolean]("Mappable").get)
            s"  mp:isMappable 'true' ;"
          else
            ""
        },
        s"""  esco:NOCClassification ${escapeString(headers("Classification identifier"))} ;
        |  esco:NOCVersion ${escapeString(headers("Classification version"))} ;
        |  esco:NOCID ${escapeString(values.getAs[String]("NOCID").get)} .""".stripMargin),
      {
        if (topConcept == "top")
          s"""<${conceptScheme}>
          |  skos:hasTopConcept <${conceptURI}> .""".stripMargin
        else
          ""
      },
      generatePrefLabel(conceptURI, prefLabel),
      generateAltLabels(conceptURI, altLabels),
      generateRelations(conceptURI, relatedNOCs)) + "\n\n"
  }

  def generateLabel(text: String, genders: Set[String], lang: Option[String]): (String, String) = {
    val (labelURI, labelUUID) = generateRandomURI("label")
    (labelURI, s"""<${labelURI}>
    |  a esco:Label, skosxl:Label ;
    |  mu:uuid "${labelUUID}" ;
    |  skosxl:literalForm ${escapeString(text, lang)} ;
    |""".stripMargin + {
      genders.map(x => s"  esco:hasLabelRole <${getLabelRole(x)}> ;\n").mkString("")
    })
  }

  def generatePrefLabel(conceptURI: String, prefLabel: (String, Set[String], Option[String])): String = {
    val (text, genders, lang) = prefLabel
    val (labelURI, turtle) = generateLabel(text, genders, lang)
    turtle + s"""  a skosthes:PreferredTerm .
    |
    |<${conceptURI}>
    |  skosxl:prefLabel ${escapeURI(labelURI)} .""".stripMargin
  }

  def generateAltLabels(conceptURI: String, altLabels: Set[(String, Set[String], Option[String])]): String = {
    altLabels.map(x => {
        val (labelURI, turtle) = generateLabel(x._1, x._2, x._3)
        turtle + s"""  a skosthes:SimpleNonPreferredTerm .
        |
        |<${conceptURI}>
        |  skosxl:altLabel ${escapeURI(labelURI)} .""".stripMargin
      })
      .mkString("\n\n")
  }

  def generateRelations(conceptURI: String, relatedNOCs: Set[String]): String = {
    relatedNOCs.map(occupation => {
        val (relationURI, relationUUID) = generateRandomURI("relation")
        s"""<${relationURI}>
        |  a esco:Relationship ;
        |  mu:uuid "${relationUUID}" ;
        |  esco:hasRelationshipType <http://data.europa.eu/esco/RelationshipType#iC.essentialSkill> ;
        |  esco:refersConcept <${conceptURI}> ;
        |  esco:isRelationshipFor <${generateURI("occupation", occupation)._1}> .""".stripMargin
      })
      .mkString("\n\n")
  }

}

class TurtleMapping(val documentID: String, val mappingEffortURI: String) extends BuilderMapping with Turtle {
  import MapExtensions._
  import sparql.SPARQLHelpers._

  def transform(it: TraversableOnce[Map[String, Any]]): TraversableOnce[Future[String]] = {
    Iterator(Future.successful(this.generateHeader)) ++ it.map(generateMapping)
  }

  // NOTE: this method shouldn't be here. It should be done during the parsing
  // of the file and return proper errors in a Try object. Unfortunately I
  // haven't foreseen that and now I'm forced to do this hack to get a proper
  // error being returned by the endpoint. See also ImportMapping.scala
  def findNOCURI(id: String, version: String): Future[String] = Config.applicationSPARQL.getQuery(
    s"""PREFIX esco: ${escapeURI(prefixes("esco"))}
      |PREFIX skos: ${escapeURI(prefixes("skos"))}
      |
      |SELECT ?x
      |FROM ${escapeURI(Config.applicationSPARQL.graph)}
      |WHERE {
      |  ?x esco:NOCID ${escapeString(id)} ;
      |  esco:NOCVersion ${escapeString(version)} ;
      |  skos:inScheme <${headers.nocURI}> .
      |}
      |""".stripMargin)
    .map {
      response =>
        val bindings = (response \ "results" \ "bindings").asInstanceOf[JsArray].value
        bindings.headOption match {
          case Some(binding) => (binding \ "x" \ "value").as[String]
          case None => throw new NOCIDNotFound(s"Can not find NOC ID: $id")
        }
    }

  def generateHeader: String = {
    s"""${generatePrefixes}
      |
      |""".stripMargin
  }

  def generateMapping(values: Map[String, Any]): Future[String] = {
    val (mappingURI, mappingUUID) = generateRandomURI("mapping")
    val escoURI = values.getAs[Uri]("ESCOURI").get.toString
    for {
      nocURI <- findNOCURI(values.getAs[String]("NOCID").get, headers.nocVersion)
      result <- Future.successful(s"""<$mappingURI>
        |  a mp:Mapping, mp:ImportMapping ;
        |  mu:uuid "$mappingUUID" ;
        |  mp:isMappingFor ${escapeURI(mappingEffortURI)} ;
        |  mp:matchType ${escapeString(values.getAs[String]("MappingType").get)} ;
        |  mp:mapsFrom ${escapeURI(if (headers.isFromEsco) escoURI else nocURI)} ;
        |  mp:mapsTo ${escapeURI(if (headers.isToEsco) escoURI else nocURI)} ;
        |  mp:status ${escapeString(values.getAs[String]("MappingStatus").getOrElse("todo"))} .
        |
        |""".stripMargin)
    } yield result
  }

  def generateMetadata: String = {
    s"""${generatePrefixes}
    |
    |<$graphURI>
    |  a esco:Graph ;
    |  mu:uuid "$graphUUID" ;
    |  esco:graph <$graph> ;
    |  esco:documentID "$documentID" ;
    |  esco:databaseURL <${Config.applicationSPARQL.crudEndpoint}> ;
    |  esco:NOCClassification ${escapeString(headers.nocIdentifier)} ;
    |  esco:NOCVersion ${escapeString(headers.nocVersion)} ;
    |  esco:status esco:NotValidated .
    |""".stripMargin
  }
}
