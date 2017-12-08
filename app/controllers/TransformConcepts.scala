package controllers

import com.roundeights.hasher.Implicits._
import play.api._
import play.api.mvc._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json._
import play.api.libs.ws._
import play.api.Play.current
import scala.concurrent.Future
import parser.{TaxonomyHeaders, ESCOConcepts}
import builder.TurtleTaxonomy

object TransformConcepts extends Controller {

  def create = Action.async(BodyParsers.parse.raw) { implicit request =>
    val (success, rest) = {
      if (request.queryString.contains("encoding"))
        ESCOConcepts.parseFile(request.body.asFile, request.queryString("encoding").head)
      else
        ESCOConcepts.parseFile(request.body.asFile)
    }

    val headersValidation = TaxonomyHeaders.validate

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
        val documentDigest: String = request.body.asFile.md5
        val builder = new TurtleTaxonomy(documentDigest)

        Future {
          Ok(builder.transform(success.map(x => x._2.get)).mkString(""))
        }
      }
    }
  }

}
