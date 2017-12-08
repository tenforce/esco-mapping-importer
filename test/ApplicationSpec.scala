import com.netaporter.uri.dsl._
import java.nio.file.Files
import org.scalatestplus.play._
import play.api.test._
import play.api.test.Helpers._
import scala.util.{Success}
import builder.TurtleTaxonomy
import parser._
import config.Config

/**
 * Add your spec here.
 * You can mock out a whole application including requests, plugins etc.
 * For more information, consult the wiki.
 */
class ApplicationSpec extends PlaySpec with OneAppPerTest {

  "Routes" should {

    "send 404 on a bad request" in  {
      route(app, FakeRequest(GET, "/")) mustBe None
    }

  }

  "Turtle String Escaping" should {
    val builder = new TurtleTaxonomy("foo")

    "wraps strings with double quotes" in {
      builder.escapeString("foo") mustBe "\"foo\""
    }
    "escapes double quotes in string" in {
      builder.escapeString("foo\"bar") mustBe "\"foo\\\"bar\""
    }
    "escapes new line feed and carriage return characters" in {
      builder.escapeString("foo\n\rbar") mustBe "\"foo\\n\\rbar\""
    }
  }

  "TSV parser" should {
    "parse TSV and return columns" in {
      val source = TSVParser.iter(Seq(
        "foo", "foo\tbar", "foo\tbar\tbaz", "\"foo\"\tbar\tba\"z\"", "\"foo\"\"bar\"\"baz\""))
      source.next._2 mustBe Array("foo")
      source.next._2 mustBe Array("foo", "bar")
      source.next._2 mustBe Array("foo", "bar", "baz")
      source.next._2 mustBe Array("foo", "bar", "ba\"z\"")
      source.next._2 mustBe Array("foo\"bar\"baz")
    }

    "parse TSV with any encoding" in {
      val f = Files.createTempFile("data", "asTest")
      f.toFile.deleteOnExit
      Files.write(f, "foo\tbar\r\nbaz".getBytes("utf-16le"))
      val source = TSVParser.parseFile(f.toFile, "utf-16le")
      source.next._2 mustBe Array("foo", "bar")
      source.next._2 mustBe Array("baz")
    }
  }

  "The column processors" should {

    "parse column NOCURI" in {
      ESCOURI.run("http://example.org").isSuccess mustBe true
      ESCOURI.run("http://hostname_with_underscore").isSuccess mustBe true
      ESCOURI.run("#URI_without_hostname").isSuccess mustBe true
      ESCOURI.run("").isSuccess mustBe false
    }

    "parse column MappingType" in {
      MappingType.run("exact").isSuccess mustBe true
      MappingType.run("broad").isSuccess mustBe true
      MappingType.run("narrow").isSuccess mustBe true
      MappingType.run("invalid").isSuccess mustBe false
    }

    "parse columin MappingStatus" in {
      MappingStatus.run("todo").isSuccess mustBe true
      MappingStatus.run("approved").isSuccess mustBe true
      MappingStatus.run("rejected").isSuccess mustBe true
      MappingStatus.run("removed").isSuccess mustBe true
      MappingStatus.run("error").isSuccess mustBe false
    }
  }

  "Mapping Headers" should {

    "require the mapping version" in {
      MappingHeaders.parse(Seq())
      val result: Option[String] = MappingHeaders.validate
      result.nonEmpty mustBe true
      result.get must include ("Mapping version")
    }

    "require a `from` URI" in {
      MappingHeaders.parse(Seq(
        (1, Array("Mapping version", "something")),
        (2, Array("To URI", "something"))))
      val result: Option[String] = MappingHeaders.validate
      result.nonEmpty mustBe true
      result.get must include ("From URI")
    }

    "require a `from` identifier and fails if version is missing" in {
      MappingHeaders.parse(Seq(
        (1, Array("Mapping version", "something")),
        (2, Array("To URI", "something")),
        (3, Array("From ID", "something"))))
      val result: Option[String] = MappingHeaders.validate
      result.nonEmpty mustBe true
      result.get must include ("From URI")
    }

    "require a `from` identifier with version & type" in {
      MappingHeaders.parse(Seq(
        (1, Array("Mapping version", "something")),
        (2, Array("To URI", "something")),
        (3, Array("From ID", "A")),
        (4, Array("From version", "B")),
        (6, Array("From type", "skill"))))
      val result: Option[String] = MappingHeaders.validate
      if (result.nonEmpty)
        result.get must not include ("From URI")
      result.isEmpty mustBe true
      MappingHeaders.nocIdentifier mustBe "A"
      MappingHeaders.nocVersion mustBe "B"
      MappingHeaders.nocType mustBe "skill"
    }

    "require a `to` URI" in {
      MappingHeaders.parse(Seq(
        (1, Array("Mapping version", "something")),
        (2, Array("From URI", "something"))))
      val result: Option[String] = MappingHeaders.validate
      result.nonEmpty mustBe true
      result.get must include ("To URI")
    }

    "require a `to` identifier and fails if version is missing" in {
      MappingHeaders.parse(Seq(
        (1, Array("Mapping version", "something")),
        (2, Array("From URI", "something")),
        (3, Array("To ID", "something"))))
      val result: Option[String] = MappingHeaders.validate
      result.nonEmpty mustBe true
      result.get must include ("To URI")
    }

    "require a `to` identifier with version & type" in {
      MappingHeaders.parse(Seq(
        (1, Array("Mapping version", "something")),
        (2, Array("From URI", "something")),
        (3, Array("To ID", "A")),
        (4, Array("To version", "B")),
        (5, Array("To type", "skill"))))
      val result: Option[String] = MappingHeaders.validate
      result.isEmpty mustBe true
      MappingHeaders.nocIdentifier mustBe "A"
      MappingHeaders.nocVersion mustBe "B"
      MappingHeaders.nocType mustBe "skill"
    }

    "disallow the `from` and `to` URI to be provided directly" in {
      MappingHeaders.parse(Seq(
        (1, Array("Mapping version", "something")),
        (2, Array("From URI", "A")),
        (3, Array("To URI", "B"))))
      val result: Option[String] = MappingHeaders.validate
      result.nonEmpty mustBe true
      result.get must include ("at least")
    }

    "disallow the `from` and `to` URI to be absent at the same time" in {
      MappingHeaders.parse(Seq(
        (1, Array("Mapping version", "something")),
        (2, Array("From ID", "A")),
        (3, Array("From version", "B")),
        (4, Array("From type", "skill")),
        (5, Array("To ID", "A")),
        (6, Array("To version", "B")),
        (7, Array("To type", "skill"))))
      val result: Option[String] = MappingHeaders.validate
      result.nonEmpty mustBe true
      result.get must include ("at least")
    }

  }

}
