package config

import java.net.URI
import scala.util.{Try, Success, Failure}
import scala.sys.SystemProperties
import play.api._
import play.api.libs.ws._
import play.api.Play.current
import com.tenforce.mu_semtech.db_support.DbSupport
import com.netaporter.uri.dsl._
import sparql.SPARQLClient

trait SPARQLClientWithRepository extends SPARQLClient {
  val baseURL: String
  val repository: String

  lazy val endpoint: String = baseURL / "repositories" / repository
  lazy val postEndpoint: String = endpoint / "statements"
  lazy val crudEndpoint: String = endpoint / "statements"

  override def crudInsertGraph(request: WSRequestHolder, graph: String): WSRequestHolder = {
    request.withQueryString("context" -> s"<$graph>")
  }
}

object ApplicationSPARQLFromDBSupport extends SPARQLClientWithRepository {
  lazy val dbSupport = new DbSupport("mu_semtech_id")
  lazy val dbConfig = dbSupport.getDbConfig()
  lazy val baseURL = Option(dbConfig.getUrl) match {
    case Some(x) => x
    case None => throw new AssertionError("Missing database URL in DbSupport")
  }
  lazy val repository = dbConfig.getId
  lazy val username = Option(dbConfig.getUser)
  lazy val password = Option(dbConfig.getPassword)
  lazy val graph = SPARQLClient.graph
  lazy val authScheme = {
    if (username.nonEmpty && password.nonEmpty)
      WSAuthScheme.DIGEST
    else
      WSAuthScheme.NONE
  }
}

object ApplicationSPARQLWithRepository extends SPARQLClientWithRepository {
  lazy val baseURL = SPARQLClient.endpoint
  lazy val repository = sys.env.get("SPARQL_REPOSITORY") match {
    case Some(x) => x
    case None => Play.configuration.getString("sparqlrepository") match {
      case Some(x) => x
      case None => throw new AssertionError("Missing database repository")
    }
  }
  lazy val username = sys.env.get("SPARQL_USER")
  lazy val password = sys.env.get("SPARQL_PASSWORD")
  lazy val graph = SPARQLClient.graph
  lazy val authScheme = {
    if (username.nonEmpty && password.nonEmpty)
      WSAuthScheme.DIGEST
    else
      WSAuthScheme.NONE
  }
}

object Config {
  val systemProperties = new SystemProperties

  lazy val applicationSPARQL = Try(ApplicationSPARQLFromDBSupport.dbSupport) match {
    case Success(x) => ApplicationSPARQLFromDBSupport
    case Failure(e) =>
      Logger.info(s"Can not load DbSupport configuration: ${e.getMessage}")
      Logger.info("Fallbacking on environment variables")
      Try(ApplicationSPARQLWithRepository.repository) match {
        case Success(_) => ApplicationSPARQLWithRepository
        case Failure(_) => SPARQLClient
      }
  }

  lazy val dataDir = sys.env.get("DATA_DIR") match {
    case Some(x) => x
    case None =>
      systemProperties.get("CONFIG_DIR_IMPORT_CONCEPTS") match {
        case Some(x) => x
        case None =>
          throw new AssertionError("Missing environment variable DATA_DIR or " +
            "system property CONFIG_DIR_IMPORT_CONCEPTS")
      }
  }

  lazy val chunkSize = sys.env.get("CHUNK_SIZE") match {
    case Some(x) =>
      Try(x.toInt) match {
        case Success(x) if (x > 0) => Some(x)
        case Success(x) if (x == 0) => None
        case _ =>
          throw new AssertionError("Environment variable CHUNK_SIZE must be a positive integer")
      }
    case None =>
      Play.configuration.getInt("chunksize") match {
        case Some(x) if (x > 0) => Some(x)
        case Some(x) if (x == 0) => None
        case Some(x) =>
          throw new AssertionError("Play configuration variable chunksize must be a positive integer")
        case None => None
      }
  }
}
