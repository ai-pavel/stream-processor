package stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.*
import io.circe.parser.*
import io.circe.syntax.*

class EventSpec extends AnyFlatSpec with Matchers:

  "Event" should "decode from JSON" in {
    val json = """{"timestamp":1000,"key":"sensor-1","payload":42.5}"""
    val event = decode[Event](json)
    event shouldBe Right(Event(1000L, "sensor-1", Json.fromDoubleOrNull(42.5)))
  }

  it should "encode to JSON" in {
    val event = Event(1000L, "sensor-1", Json.fromDoubleOrNull(42.5))
    val json = event.asJson.noSpaces
    json should include("\"timestamp\":1000")
    json should include("\"key\":\"sensor-1\"")
  }

  it should "extract numeric value from payload number" in {
    val event = Event(1000L, "k", Json.fromDoubleOrNull(3.14))
    Event.numericValue(event) shouldBe Some(3.14)
  }

  it should "extract numeric value from payload object with value field" in {
    val event = Event(1000L, "k", Json.obj("value" -> Json.fromDoubleOrNull(7.0)))
    Event.numericValue(event) shouldBe Some(7.0)
  }

  it should "return None for non-numeric payload" in {
    val event = Event(1000L, "k", Json.fromString("hello"))
    Event.numericValue(event) shouldBe None
  }

  it should "return None for null payload" in {
    val event = Event(1000L, "k", Json.Null)
    Event.numericValue(event) shouldBe None
  }

  it should "return None for boolean payload" in {
    val event = Event(1000L, "k", Json.fromBoolean(true))
    Event.numericValue(event) shouldBe None
  }

  it should "return None for array payload" in {
    val event = Event(1000L, "k", Json.arr(Json.fromInt(1), Json.fromInt(2)))
    Event.numericValue(event) shouldBe None
  }

  it should "return None for object payload without value field" in {
    val event = Event(1000L, "k", Json.obj("name" -> Json.fromString("test")))
    Event.numericValue(event) shouldBe None
  }

  it should "extract integer value from payload" in {
    val event = Event(1000L, "k", Json.fromInt(42))
    Event.numericValue(event) shouldBe Some(42.0)
  }

  it should "extract large numeric value from payload" in {
    val event = Event(1000L, "k", Json.fromLong(1000000L))
    Event.numericValue(event) shouldBe Some(1000000.0)
  }

  it should "handle negative numeric payload" in {
    val event = Event(1000L, "k", Json.fromDoubleOrNull(-99.9))
    Event.numericValue(event) shouldBe Some(-99.9)
  }

  it should "handle zero numeric payload" in {
    val event = Event(1000L, "k", Json.fromDoubleOrNull(0.0))
    Event.numericValue(event) shouldBe Some(0.0)
  }

  it should "decode from JSON with object payload" in {
    val json = """{"timestamp":500,"key":"k1","payload":{"value":10}}"""
    val event = decode[Event](json)
    event.isRight shouldBe true
    event.toOption.get.timestamp shouldBe 500L
    event.toOption.get.key shouldBe "k1"
  }

  it should "fail to decode invalid JSON" in {
    val json = """{"timestamp":"not_a_number","key":"k1","payload":42}"""
    val event = decode[Event](json)
    event.isLeft shouldBe true
  }

  it should "fail to decode JSON missing required fields" in {
    val json = """{"timestamp":1000}"""
    val event = decode[Event](json)
    event.isLeft shouldBe true
  }

  it should "roundtrip encode and decode" in {
    val original = Event(9999L, "roundtrip-key", Json.fromDoubleOrNull(3.14))
    val encoded = original.asJson
    val decoded = encoded.as[Event]
    decoded shouldBe Right(original)
  }

  it should "encode all fields correctly" in {
    val event = Event(42L, "test-key", Json.obj("a" -> Json.fromInt(1)))
    val json = event.asJson
    json.hcursor.downField("timestamp").as[Long] shouldBe Right(42L)
    json.hcursor.downField("key").as[String] shouldBe Right("test-key")
    json.hcursor.downField("payload").focus.isDefined shouldBe true
  }

  "some extension" should "wrap value in Some" in {
    import Event.some
    42.some shouldBe Some(42)
    "hello".some shouldBe Some("hello")
  }
