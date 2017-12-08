package queries

import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json._
import scala.concurrent.{Future, Promise}
import config.Config
import sparql._
import parser.{MappingHeaders => Headers}

class InvalidMapping(val message: String) extends Exception

object MappingQueries {
  import SPARQLHelpers._

  def findMappingEffortURI(id: String): Future[Option[String]] = Config.applicationSPARQL.getQuery(
    s"""PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      |PREFIX mp: <http://sem.tenforce.com/vocabularies/mapping-platform/>
      |
      |SELECT ?x
      |FROM ${escapeURI(Config.applicationSPARQL.graph)}
      |WHERE {
      |  ?x a mp:MappingEffort ;
      |  mu:uuid ${escapeString(id)} .
      |}
      |""".stripMargin)
    .map {
      response =>
        val bindings = (response \ "results" \ "bindings").asInstanceOf[JsArray].value
        bindings.headOption match {
          case Some(binding) => Some((binding \ "x" \ "value").as[String])
          case None => None
        }
    }

  // NOTE: the specifications changed when we realized that the frontend
  // provides the mapping effort UUID in input despite we already have the
  // concept schemes and the direction in the file headers. Since we received
  // the mapping effort and the concept schemes from the headers, it is
  // possible that they are in conflict.
  def ensureMappingEffortMatch(mappingEffortURI: String) = Config.applicationSPARQL.getQuery(
    s"""PREFIX mp: <http://sem.tenforce.com/vocabularies/mapping-platform/>
      |
      |ASK
      |FROM ${escapeURI(Config.applicationSPARQL.graph)}
      |{
      |  ${escapeURI(mappingEffortURI)} a mp:MappingEffort ;
      |  mp:taxonomyFrom ${escapeURI(if (Headers.isFromEsco) Headers.escoURI else Headers.nocURI)} ;
      |  mp:taxonomyTo ${escapeURI(if (Headers.isToEsco) Headers.escoURI else Headers.nocURI)} .
      |}
      |""".stripMargin)
    .map { response =>
      if( !(response \ "boolean").as[Boolean] )
        throw new InvalidMapping(
          s"The mappings do not match the mapping effort selected. " +
          {
            if (Headers.isToEsco)
              s"These mappings are from NOC classifcation ${Headers.nocIdentifier} " +
              s"(${Headers.nocVersion}) to ESCO. "
            else
              s"These mappings are from ESCO to NOC classifcation ${Headers.nocIdentifier} " +
              s"(${Headers.nocVersion}). "
          } +
          "Please ensure the direction of the mapping is correct.")
    }
}
