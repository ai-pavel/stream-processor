package stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.Json

class SourceSpec extends AnyFlatSpec with Matchers:

  "SeqSource" should "emit all events in order" in {
    val events = Seq(
      Event(1000, "a", Json.fromInt(1)),
      Event(2000, "b", Json.fromInt(2)),
      Event(3000, "c", Json.fromInt(3))
    )
    val source = SeqSource(events)
    val collected = scala.collection.mutable.ArrayBuffer.empty[Event]
    source.readEvents(e => collected += e)
    collected.toSeq shouldBe events
  }

  it should "handle empty sequence" in {
    val source = SeqSource(Seq.empty)
    val collected = scala.collection.mutable.ArrayBuffer.empty[Event]
    source.readEvents(e => collected += e)
    collected shouldBe empty
  }

  it should "handle single event" in {
    val event = Event(1000, "k", Json.fromInt(42))
    val source = SeqSource(Seq(event))
    val collected = scala.collection.mutable.ArrayBuffer.empty[Event]
    source.readEvents(e => collected += e)
    collected.toSeq shouldBe Seq(event)
  }

  it should "handle events with various payload types" in {
    val events = Seq(
      Event(100, "k", Json.fromInt(1)),
      Event(200, "k", Json.fromDoubleOrNull(2.5)),
      Event(300, "k", Json.fromString("text")),
      Event(400, "k", Json.obj("value" -> Json.fromInt(10))),
      Event(500, "k", Json.Null),
      Event(600, "k", Json.arr(Json.fromInt(1), Json.fromInt(2)))
    )
    val source = SeqSource(events)
    val collected = scala.collection.mutable.ArrayBuffer.empty[Event]
    source.readEvents(e => collected += e)
    collected.toSeq shouldBe events
  }

  it should "handle duplicate events" in {
    val event = Event(1000, "k", Json.fromInt(1))
    val events = Seq(event, event, event)
    val source = SeqSource(events)
    val collected = scala.collection.mutable.ArrayBuffer.empty[Event]
    source.readEvents(e => collected += e)
    collected should have size 3
  }
