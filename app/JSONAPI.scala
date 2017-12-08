package jsonapi

import com.netaporter.uri.Uri
import play.api.data.validation.ValidationError
import play.api.libs.json._
import scala.util.{Try, Success, Failure}

object JSONAPIHelpers {
  implicit object UriReads extends Reads[Uri] {
    def reads(json: JsValue) = json match {
      case JsString(s) => Try(Uri.parse(s)) match {
        case Success(uri) => JsSuccess(uri)
        case Failure(e) => JsError(Seq(
          JsPath() -> Seq(ValidationError("error.expected.uri.error", e.getMessage))))
      }
      case _ => JsError(Seq(JsPath() -> Seq(ValidationError("error.expected.jsstring"))))
    }
  }

  implicit object UriWrites extends Writes[Uri] {
    def writes(o: Uri) = JsString(o.toString)
  }

  def formatValidationErrors(toValidate: JsResult[Any]*): Option[JsObject] = {
    val errors = toValidate
      .filter(_.isError)
      .map(x => x.asInstanceOf[JsError])
      .flatMap(_.errors)
    if( errors.nonEmpty )
    {
      Some(JsObject(Seq(
        "errors" -> JsArray(
          errors.flatMap {
            error => error._2.map(e => JsObject(Seq(
              "title" -> JsString(e.message),
              "detail" -> JsString(s"invalid value for ${error._1}: ${e.args.mkString("\n")}"))))
          }
          ))))
    }
    else None
  }
}
