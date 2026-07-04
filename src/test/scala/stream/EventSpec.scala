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
