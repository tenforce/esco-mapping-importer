package controllers

import java.io.{BufferedWriter, FileWriter}
import play.api._
import play.api.mvc._
import play.api.libs.Files.TemporaryFile
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json._
import play.api.Play.current
import scala.concurrent.{Future, Promise}
import parser.{MappingHeaders, Mapping}
import builder._
import config.Config
import scalax.file.Path
import queries._

object ImportMapping extends Controller {

  import MappingQueries._

  def backup(request: Request[RawBuffer], hash: String) = {
    import scalax.file.ImplicitConversions._

    val dest = Config.dataDir / "backup" / s"$hash.tsv"
    if (!dest.exists)
      Path(request.body.asFile).copyTo(dest)
  }

  def preparePayload(builder: TurtleMapping, data: Iterator[Map[String, Any]]): TraversableOnce[Future[TemporaryFile]] = {
    val chunks = Config.chunkSize match {
      case Some(x) => data.sliding(x, x)
      case None => Seq(data)
    }

    chunks
      .map(builder.transform)
      .flatMap {
        chunk =>
          Seq(for {
            tmp <- Future.successful(TemporaryFile("data", "asTurtle"))
            writer <- Future.successful(new BufferedWriter(new FileWriter(tmp.file)))
            writeChunks <- chunk.foldLeft(Future(List.empty[Any]))((prevFuture, item) => {
              for {
                prev <- prevFuture
                curr <- item.map {
                  result => writer.write(result)
                }
              } yield prev :+ curr
            })
            closeTmp <- Future.successful(writer.close)
          } yield tmp)
      }

  }

  def uploadPayload(builder: TurtleMapping, tmpFiles: TraversableOnce[Future[TemporaryFile]]): Future[Any] = {
    import com.netaporter.uri.dsl._

    val tempGraph = builder.graph

    for {
      deleteData <- Config.applicationSPARQL.delete(tempGraph)
        .map(Some(_)).recover { case ex => None }
      // NOTE: we do not parallelize the HTTP calls here because Virtuoso
      // doesn't seem to support it very well
      uploadData <- tmpFiles.foldLeft(Future(List.empty[Any]))((prevFuture, futureTmp) => {
        for {
          prev <- prevFuture
          curr <- futureTmp.map {
            tmp => Config.applicationSPARQL.update(tempGraph, tmp.file, builder.mimeType)
          }
        } yield prev :+ curr
      })
      uploadMetadata <- Config.applicationSPARQL.update(
        Config.applicationSPARQL.graph, builder.generateMetadata, builder.mimeType)
    } yield uploadData
  }

  def create = Action.async(BodyParsers.parse.raw) { implicit request =>
    import com.roundeights.hasher.Implicits._

    val (success, rest) = {
      if (request.queryString.contains("encoding"))
        Mapping.parseFile(request.body.asFile, request.queryString("encoding").head)
      else
        Mapping.parseFile(request.body.asFile)
    }
    val headersValidation = MappingHeaders.validate
    val documentDigest: String = request.body.asFile.md5

    if (!request.queryString.contains("uuid"))
      Future { BadRequest( JsObject(Seq(
        "errors" -> JsArray(Seq(JsObject(Seq(
          "title" -> JsString("Missing parameter uuid"),
          "detail" -> JsString("Missing mapping effort UUID")))))
      )))}
    else if (headersValidation.nonEmpty)
      Future { BadRequest( JsObject(Seq(
        "errors" -> JsArray(Seq(JsObject(Seq(
          "title" -> JsString("Header parsing error"),
          "detail" -> JsString(headersValidation.get)))))
      )))}
    else if (!success.hasNext && !rest.hasNext)
      Future { NoContent }
    else
    {
      val p = Promise[Result]()
      val mappingEffortUUID = request.queryString("uuid").head

      val f = for {
        mappingEffortURI <- findMappingEffortURI(mappingEffortUUID) map { result =>
          result match {
            case Some(x) => x
            case None =>
              p.success(BadRequest( JsObject(Seq(
                "errors" -> JsArray(Seq(
                  JsObject(Seq(
                    "title" -> JsString("Can not find mapping effort"),
                    "detail" -> JsString(
                      s"Can not find mapping effort UUID ${mappingEffortUUID}")))
              ))))))
              throw new Throwable("Can't find mapping effort URI")
          }
        }
        check <- ensureMappingEffortMatch(mappingEffortURI) recover {
          case ex: InvalidMapping =>
            p.success(BadRequest( JsObject(Seq(
              "errors" -> JsArray(Seq(JsObject(Seq(
                "title" -> JsString("Mapping effort does not match"),
                "detail" -> JsString(ex.message)))))))))
            throw ex
        }
        builder <- Future.successful(new TurtleMapping(documentDigest, mappingEffortURI))
        tmpFiles <- Future {
          val tmpFiles = preparePayload(builder, success.map(_._2.get))
          if (rest.hasNext)
          {
            val someFailures = rest
              .take(Play.configuration.getInt("lookuplinesafterfailure").getOrElse(1))
              .filter(_._2.isFailure)
            p.success(BadRequest( JsObject(Seq(
              "errors" -> JsArray(
                someFailures.map(
                    x => JsObject(Seq(
                      "title" -> JsString("Content parsing error"),
                      "detail" -> JsString(s"Line ${x._1}: ${x._2.failed.get.getMessage}"))))
                  .toSeq)
            ))))
            throw new Throwable("Parsing errors")
          }
          else
            tmpFiles
        }
        backup <- Future.successful(backup(request, documentDigest))
        upload <- uploadPayload(builder, tmpFiles) recover {
          // NOTE: This exception handling is clearly a hack because I didn't foresee that we may
          // need Future during the parsing.
          case ex: NOCIDNotFound =>
            p.success(BadRequest( JsObject(Seq(
              "errors" -> JsArray(Seq(JsObject(Seq(
                "title" -> JsString("NOC ID not found"),
                "detail" -> JsString(ex.message)))))))))
            throw ex
        }
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
