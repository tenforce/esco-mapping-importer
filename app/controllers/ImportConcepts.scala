package controllers

import java.io.{BufferedWriter, FileWriter}
import play.api._
import play.api.mvc._
import play.api.libs.Files.TemporaryFile
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json._
import play.api.Play.current
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scalax.file.Path
import parser.{TaxonomyHeaders, ESCOConcepts}
import builder._
import config.Config
import sparql._

object ImportConcepts extends Controller {

  import SPARQLHelpers._

  class ConceptSchemeExists(val message: String) extends Exception

  def backup(request: Request[RawBuffer], hash: String) = {
    import scalax.file.ImplicitConversions._

    val dest = Config.dataDir / "backup" / s"$hash.tsv"
    if (!dest.exists)
      Path(request.body.asFile).copyTo(dest)
  }

  def ensureConceptSchemeNotExists(builder: TurtleTaxonomy): Future[Any] = {
    Config.applicationSPARQL.getQuery(
      s"""ASK
        |FROM ${escapeURI(Config.applicationSPARQL.graph)}
        |WHERE {
        |  {
        |    ${escapeURI(builder.occupationScheme)} ?x ?y
        |  }
        |  UNION
        |  {
        |    ${escapeURI(builder.skillScheme)} ?x ?y
        |  }
        |  UNION
        |  {
        |    ${escapeURI(builder.qualificationScheme)} ?x ?y
        |  }
        |}
        |""".stripMargin)
      .map {
        response =>
          if( (response \ "boolean").as[Boolean] )
            throw new ConceptSchemeExists(
              "Some of these concept schemes already exists: "
              + Seq(builder.occupationScheme, builder.skillScheme, builder.qualificationScheme)
                .mkString(", "))
      }
  }

  def preparePayload(builder: TurtleTaxonomy, data: Iterator[Map[String, Any]]): mutable.Queue[TemporaryFile] = {
    val tmpFiles = mutable.Queue.empty[TemporaryFile]

    while (data.hasNext) {
      // NOTE: I make a temporary file here because we need to stream the POST
      // of the data and in Play framework 2.3.x it is only possible by using a
      // File object. Unfortunately I'm forced to make a file on the disk to
      // get a file object that I can post
      val tmp = TemporaryFile("data", "asTurtle")
      val writer = new BufferedWriter(new FileWriter(tmp.file))

      val chunk = Config.chunkSize match {
        case Some(x) => builder.transform(data.map(x => x).take(x))
        case None => builder.transform(data.map(x => x))
      }
      chunk.foreach {
          case result => writer.write(result)
        }
      writer.close

      tmpFiles += tmp
    }

    tmpFiles
  }

  def uploadPayload(builder: TurtleTaxonomy, tmpFiles: mutable.Queue[TemporaryFile]): Future[Any] = {
    import com.netaporter.uri.dsl._

    val tempGraph = builder.graph

    for {
      deleteData <- Config.applicationSPARQL.delete(tempGraph)
        .map(Some(_)).recover { case ex => None }
      // NOTE: we do not parallelize the HTTP calls here because Virtuoso
      // doesn't seem to support it very well
      uploadData <- tmpFiles.foldLeft(Future(List.empty[Any]))((prevFuture, tmp) => {
        for {
          prev <- prevFuture
          curr <- Config.applicationSPARQL.update(tempGraph, tmp.file, builder.mimeType)
        } yield prev :+ curr
      })
      uploadMetadata <- Config.applicationSPARQL.update(
        Config.applicationSPARQL.graph, builder.generateMetadata, builder.mimeType)
    } yield uploadMetadata
  }

  def create = Action.async(BodyParsers.parse.raw) { implicit request =>
    import com.roundeights.hasher.Implicits._

    val (success, rest) = {
      if (request.queryString.contains("encoding"))
        ESCOConcepts.parseFile(request.body.asFile, request.queryString("encoding").head)
      else
        ESCOConcepts.parseFile(request.body.asFile)
    }
    val headersValidation = TaxonomyHeaders.validate
    val documentDigest: String = request.body.asFile.md5
    val builder = new TurtleTaxonomy(documentDigest)

    if (headersValidation.nonEmpty)
      Future { BadRequest( JsObject(Seq(
        "errors" -> JsArray(Seq(JsObject(Seq(
          "title" -> JsString("Header parsing error"),
          "detail" -> JsString(headersValidation.get)))))
      )))}
    else if (!success.hasNext && !rest.hasNext)
      Future { NoContent }
    else
    {

      val tmpFiles = preparePayload(builder, success.map(_._2.get))

      if (rest.hasNext)
      {
        val someFailures = rest
          .take(Play.configuration.getInt("lookuplinesafterfailure").getOrElse(1))
          .filter(_._2.isFailure)
        Future { BadRequest( JsObject(Seq(
          "errors" -> JsArray(
            someFailures.map(
                x => JsObject(Seq(
                  "title" -> JsString("Content parsing error"),
                  "detail" -> JsString(s"Line ${x._1}: ${x._2.failed.get.getMessage}"))))
              .toSeq)
        )))}
      }
      else
      {
        val p = Promise[Result]()

        val f = for {
          backup <- Future {
            backup(request, documentDigest)
          }
          check <- ensureConceptSchemeNotExists(builder) recover {
            case ex: ConceptSchemeExists =>
              p.success(BadRequest( JsObject(Seq(
                "errors" -> JsArray(Seq(JsObject(Seq(
                  "title" -> JsString("Concept scheme already exists"),
                  "detail" -> JsString(ex.message)))))
              ))))
              throw new Throwable("Check failed")
          }
          upload <- uploadPayload(builder, tmpFiles)
          result <- Future.successful(p.success(
            Ok(JsObject(Seq("id" -> JsString(builder.graphUUID.toString))))))
        } yield result

        f recover {
          case e => p.failure(e)
        }

        p.future
      }
    }
  }

}
