package sparql

import com.netaporter.uri.Uri
import java.io.{File, BufferedOutputStream, FileOutputStream}
import java.net.URLEncoder
import java.util.Locale
import play.api.http.{Writeable, ContentTypeOf}
import play.api.{Play, Logger}
import play.api.Play.current
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.Files.TemporaryFile
import play.api.libs.iteratee.{Enumerator, Iteratee}
import play.api.libs.json._
import play.api.libs.ws._
import scala.concurrent.Future

object SPARQLHelpers {

  def escapeString(string: String): String = {
    "\"" + string.flatMap(_ match {
      case '\"' => "\\\""
      case '\\' => "\\\\"
      case '\u000a' => "\\n"
      case '\u000d' => "\\r"
      case c => c.toString
    }) + "\""
  }

  def escapeString(string: String, lang: String): String = {
    if (Locale.getISOLanguages.find(_.==(lang)).isEmpty)
      throw new RuntimeException(s"Not a valid ISO 639-1 language: $lang")
    "\"" + string.flatMap(_ match {
      case '\"' => "\\\""
      case '\\' => "\\\\"
      case '\u000a' => "\\n"
      case '\u000d' => "\\r"
      case c => c.toString
    }) + "\"@" + lang
  }

  def escapeURI(uri: String): String = "<" + Uri.parse(uri).toString + ">"

  implicit val contentTypeOf_String = ContentTypeOf[String](Some("text/turtle"))

}

trait SPARQLClient {

  val graph: String
  val endpoint: String
  val postEndpoint: String
  val crudEndpoint: String
  val username: Option[String]
  val password: Option[String]
  val authScheme: WSAuthScheme

  def crudInsertGraph(request: WSRequestHolder, graph: String): WSRequestHolder

  var wsClient: WSClient = null

  def ensureValidHost(location: String) = {
    val uri = new java.net.URI(location)
    if (uri.getHost() == null)
      throw new AssertionError(s"Invalid hostname for database in URL: ${location}. " +
        "Hint: characters like '_' are invalids.")
  }

  lazy val requestBuilder = {
    ensureValidHost(endpoint)
    if( wsClient == null)
      wsClient = WS.client
    wsClient
      .url(endpoint)
      .withAuth(username.getOrElse(""), password.getOrElse(""), authScheme)
  }

  lazy val postRequestBuilder = {
    ensureValidHost(postEndpoint)
    if( wsClient == null)
      wsClient = WS.client
    wsClient
      .url(postEndpoint)
      .withAuth(username.getOrElse(""), password.getOrElse(""), authScheme)
  }

  lazy val crudRequestBuilder = {
    ensureValidHost(crudEndpoint)
    if( wsClient == null)
      wsClient = WS.client
    wsClient
      .url(crudEndpoint)
      .withAuth(username.getOrElse(""), password.getOrElse(""), authScheme)
  }

  private def isResponseValid(response: WSResponse): Boolean = {
    response.status >= 200 && response.status < 300
  }

  private def isResponseValid(response: WSResponseHeaders): Boolean = {
    response.status >= 200 && response.status < 300
  }

  private def toJSON(response: WSResponse): JsValue = {
    if (!isResponseValid(response))
    {
      Logger.debug(s"Request failed: ${response.statusText}\n" +
        response.body)
      throw new RuntimeException(s"Request failed: ${response.statusText}")
    }
    else
    {
      val headers: Map[String, String] = response.allHeaders.map(x => (x._1, x._2.last))
      headers.get("Content-Type") match {
        case None => JsNull
        case Some(x) if x.contains("json") => response.json
        case Some(mime) =>
          Logger.debug(s"Request succeeded but the result is not JSON but $mime:\n"
            + response.body)
          throw new RuntimeException("Request succeeded but the result is not JSON")
      }
    }
  }

  private def toString(response: WSResponse): String = {
    if (!isResponseValid(response))
      throw new RuntimeException(s"Cannot get data: ${response.statusText}")
    else
      response.body
  }

  private def toTemporaryFile(headers: WSResponseHeaders, body: Enumerator[Array[Byte]]): Future[TemporaryFile] = {
    if (!isResponseValid(headers))
      throw new RuntimeException(s"Cannot get data: ${headers.status}")

    val f = TemporaryFile("data", "fromCRUD")
    val writer = new BufferedOutputStream(new FileOutputStream(f.file))
    (body |>>> Iteratee.foreach[Array[Byte]](bytes => writer.write(bytes)))
      .map {
        _ =>
          writer.close
          f
      }
  }

  def getQuery(query: String): Future[JsValue] = {
    Logger.debug(s"Sending GET query to $endpoint:\n$query")
    requestBuilder
      .withHeaders("Accept" -> "application/json")
      .withQueryString("query" -> query)
      .get
      .map(toJSON(_))
  }

  def postQuery(query: String): Future[JsValue] = {
    Logger.debug(s"Sending POST query to $postEndpoint\n$query")
    postRequestBuilder
      .withHeaders(
        "Accept" -> "application/json",
        "Content-Type" -> "application/x-www-form-urlencoded; charset=UTF-8")
      .post(s"update=${URLEncoder.encode(query, "UTF-8")}")
      .map(toJSON(_))
  }

  def create[T](graph: String, data: T)
      (implicit wrt: Writeable[T], ct: ContentTypeOf[T]): Future[String] = {
    Logger.debug(s"Creating graph $graph on $crudEndpoint")
    crudInsertGraph(crudRequestBuilder, graph)
      .put(data)
      .map(toString)
  }

  def create[T](graph: String, data: T, contentType: String)
      (implicit wrt: Writeable[T], ct: ContentTypeOf[T]): Future[String] = {
    Logger.debug(s"Creating graph $graph on $crudEndpoint")
    crudInsertGraph(crudRequestBuilder, graph)
      .withHeaders("Content-Type" -> contentType)
      .put(data)
      .map(toString)
  }

  def create(graph: String, data: File, contentType: String): Future[String] = {
    Logger.debug(s"Creating graph $graph on $crudEndpoint")
    crudInsertGraph(crudRequestBuilder, graph)
      .withHeaders("Content-Type" -> contentType)
      .put(data)
      .map(toString)
  }

  def read(graph: String, accept: String = "text/turtle"): Future[String] = {
    Logger.debug(s"Getting graph $graph from $crudEndpoint")
    crudInsertGraph(crudRequestBuilder, graph)
      .withHeaders("Accept" -> accept)
      .get
      .map(toString)
  }

  def readToTemporaryFile(graph: String, accept: String = "text/turtle"): Future[TemporaryFile] = {
    Logger.debug(s"Getting graph $graph from $crudEndpoint")
    crudInsertGraph(crudRequestBuilder, graph)
      .withHeaders("Accept" -> accept)
      .getStream
      .flatMap(x => toTemporaryFile(x._1, x._2))
  }

  def update[T](graph: String, data: T)
      (implicit wrt: Writeable[T], ct: ContentTypeOf[T]): Future[String] = {
    Logger.debug(s"Updating graph $graph on $crudEndpoint")
    crudInsertGraph(crudRequestBuilder, graph)
      .post(data)
      .map(toString)
  }

  def update[T](graph: String, data: T, contentType: String)
      (implicit wrt: Writeable[T], ct: ContentTypeOf[T]): Future[String] = {
    Logger.debug(s"Updating graph $graph on $crudEndpoint")
    crudInsertGraph(crudRequestBuilder, graph)
      .withHeaders("Content-Type" -> contentType)
      .post(data)
      .map(toString)
  }

  def update(graph: String, data: File, contentType: String): Future[String] = {
    Logger.debug(s"Updating graph $graph on $crudEndpoint")
    crudInsertGraph(crudRequestBuilder, graph)
      .withHeaders("Content-Type" -> contentType)
      .post(data)
      .map(toString)
  }

  def delete(graph: String): Future[String] = {
    Logger.debug(s"Deleting graph $graph on $crudEndpoint")
    crudInsertGraph(crudRequestBuilder, graph)
      .delete
      .map(toString)
  }

}

object SPARQLClient extends SPARQLClient {

  lazy val graph = sys.env.get("GRAPH") match {
    case Some(x) => x
    case None => Play.configuration.getString("graph") match {
      case Some(x) => x
      case None =>
        throw new AssertionError("Missing environment variable GRAPH or app configuration `graph`")
    }
  }

  lazy val endpoint = sys.env.get("SPARQL_ENDPOINT") match {
    case Some(x) => x
    case None => Play.configuration.getString("sparqlendpoint") match {
      case Some(x) => x
      case None =>
        throw new AssertionError("Missing environment variable SPARQL_ENDPOINT or " +
          "app configuration `sparqlendpoint`")
    }
  }

  lazy val postEndpoint = endpoint

  lazy val crudEndpoint = sys.env.get("SPARQL_CRUD_ENDPOINT") match {
    case Some(x) => x
    case None => endpoint
  }

  val username = None
  val password = None
  val authScheme = WSAuthScheme.NONE

  def crudInsertGraph(request: WSRequestHolder, graph: String): WSRequestHolder = {
    request.withQueryString("graph-uri" -> graph)
  }

}
