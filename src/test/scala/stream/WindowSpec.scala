package stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.Json

class WindowSpec extends AnyFlatSpec with Matchers:

  "Tumbling window" should "assign events to non-overlapping windows" in {
    val strategy = WindowStrategy.Tumbling(1000)
    WindowAssigner.assign(strategy, 500) shouldBe Seq(TimeWindow(0, 1000))
    WindowAssigner.assign(strategy, 1500) shouldBe Seq(TimeWindow(1000, 2000))
    WindowAssigner.assign(strategy, 2000) shouldBe Seq(TimeWindow(2000, 3000))
  }

  "Sliding window" should "assign events to overlapping windows" in {
    val strategy = WindowStrategy.Sliding(1000, 500)
    val windows = WindowAssigner.assign(strategy, 750)
    windows should have size 2
    windows should contain(TimeWindow(0, 1000))
    windows should contain(TimeWindow(500, 1500))
  }

  it should "handle event at window boundary" in {
    val strategy = WindowStrategy.Sliding(1000, 500)
    val windows = WindowAssigner.assign(strategy, 1000)
    windows should contain(TimeWindow(500, 1500))
    windows should contain(TimeWindow(1000, 2000))
  }

  "Session window tracker" should "group nearby events into a session" in {
    val tracker = SessionWindowTracker(500)
    val e1 = Event(1000, "a", Json.fromInt(1))
    val e2 = Event(1200, "a", Json.fromInt(2))
    val e3 = Event(1400, "a", Json.fromInt(3))

    tracker.add(e1) shouldBe empty
    tracker.add(e2) shouldBe empty
    tracker.add(e3) shouldBe empty

    val flushed = tracker.flushAll()
    flushed should have size 1
    flushed.head._1 shouldBe "a"
    flushed.head._3 should have size 3
  }

  it should "close a session when gap is exceeded" in {
    val tracker = SessionWindowTracker(500)
    val e1 = Event(1000, "a", Json.fromInt(1))
    val e2 = Event(1200, "a", Json.fromInt(2))
    val e3 = Event(2000, "a", Json.fromInt(3)) // gap > 500ms from e2

    tracker.add(e1) shouldBe empty
    tracker.add(e2) shouldBe empty
    val closed = tracker.add(e3)
    closed should have size 1
    closed.head._2 should have size 2
  }

  it should "track sessions independently per key" in {
    val tracker = SessionWindowTracker(500)
    tracker.add(Event(1000, "a", Json.fromInt(1)))
    tracker.add(Event(1000, "b", Json.fromInt(2)))

    val flushed = tracker.flushAll()
    flushed should have size 2
    flushed.map(_._1).toSet shouldBe Set("a", "b")
  }

  "TimeWindow" should "report containment correctly" in {
    val w = TimeWindow(100, 200)
    w.contains(100) shouldBe true
    w.contains(150) shouldBe true
    w.contains(200) shouldBe false
    w.contains(99) shouldBe false
  }
