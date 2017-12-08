package controllers

import com.roundeights.hasher.Implicits._
import play.api._
import play.api.mvc._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json._
import play.api.libs.ws._
import play.api.Play.current
import scala.concurrent.{Future, Promise}
import parser.{MappingHeaders, Mapping}
import builder.TurtleMapping
import queries._

object TransformToMapping extends Controller {

  import MappingQueries._

  def create = Action.async(BodyParsers.parse.raw) { implicit request =>
    val (success, rest) = {
      if (request.queryString.contains("encoding"))
        Mapping.parseFile(request.body.asFile, request.queryString("encoding").head)
      else
        Mapping.parseFile(request.body.asFile)
    }

    val headersValidation = MappingHeaders.validate

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
        val mappingEffortUUID = request.queryString("uuid").head
        val documentDigest: String = request.body.asFile.md5

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
          result <- Future {
            builder
              .transform(success.map(x => x._2.get))
              .foldLeft(Future(List.empty[String]))((prevFuture, item) => {
                for {
                  prev <- prevFuture
                  curr <- item
                } yield prev :+ curr
              }).map {
                results =>
                  p.success(Ok(results.mkString("")))
              }
          }
        } yield result

        f recover {
          case e => p.failure(e)
        }

        p.future
      }
    }
  }

}
