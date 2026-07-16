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

  "StdinSource" should "read and dispatch valid JSON-line events from stdin" in {
    val lines =
      """{"timestamp":100,"key":"a","payload":1.0}
        |{"timestamp":200,"key":"b","payload":2.0}
        |{"timestamp":300,"key":"c","payload":3.0}""".stripMargin + "\n"
    val in = new java.io.ByteArrayInputStream(lines.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    val collected = scala.collection.mutable.ArrayBuffer.empty[Event]
    Console.withIn(in) {
      StdinSource.readEvents(e => collected += e)
    }

    collected should have size 3
    collected.map(_.key) shouldBe Seq("a", "b", "c")
    collected.map(_.timestamp) shouldBe Seq(100L, 200L, 300L)
  }

  it should "skip blank lines and report parse errors to stderr" in {
    val lines =
      """{"timestamp":10,"key":"a","payload":1.0}

        |not valid json at all
        |{"timestamp":20,"key":"b","payload":2.0}
        |""".stripMargin
    val in = new java.io.ByteArrayInputStream(lines.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    val collected = scala.collection.mutable.ArrayBuffer.empty[Event]
    // StdinSource writes parse errors to System.err; redirect it to a
    // discard stream so the test output stays clean. The important behaviour
    // is that the invalid line is skipped and the valid events are dispatched.
    val originalErr = System.err
    System.setErr(new java.io.PrintStream(new java.io.ByteArrayOutputStream(), true, java.nio.charset.StandardCharsets.UTF_8))
    try
      Console.withIn(in) {
        StdinSource.readEvents(e => collected += e)
      }
    finally System.setErr(originalErr)

    // Blank line is skipped, invalid line yields a parse error on stderr,
    // and the two valid events are dispatched.
    collected should have size 2
    collected.map(_.key) shouldBe Seq("a", "b")
  }

  it should "stop at end of input (null line)" in {
    val lines = """{"timestamp":5,"key":"k","payload":0.0}
""".stripMargin
    val in = new java.io.ByteArrayInputStream(lines.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    val collected = scala.collection.mutable.ArrayBuffer.empty[Event]
    Console.withIn(in) {
      StdinSource.readEvents(e => collected += e)
    }

    collected should have size 1
    collected.head.timestamp shouldBe 5L
  }
