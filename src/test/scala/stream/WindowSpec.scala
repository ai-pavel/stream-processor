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

  it should "compute duration" in {
    val w = TimeWindow(100, 200)
    w.durationMs shouldBe 100L
  }

  it should "compute zero duration for same start and end" in {
    val w = TimeWindow(100, 100)
    w.durationMs shouldBe 0L
  }

  it should "compute large duration" in {
    val w = TimeWindow(0, 1000000L)
    w.durationMs shouldBe 1000000L
  }

  "Tumbling window" should "assign event at window boundary start" in {
    val strategy = WindowStrategy.Tumbling(1000)
    WindowAssigner.assign(strategy, 0) shouldBe Seq(TimeWindow(0, 1000))
  }

  it should "assign event at exact multiple" in {
    val strategy = WindowStrategy.Tumbling(1000)
    WindowAssigner.assign(strategy, 3000) shouldBe Seq(TimeWindow(3000, 4000))
  }

  it should "always assign exactly one window" in {
    val strategy = WindowStrategy.Tumbling(500)
    for ts <- Seq(0L, 1L, 499L, 500L, 999L, 1000L) do
      WindowAssigner.assign(strategy, ts) should have size 1
  }

  "Sliding window" should "assign event to single window when slide equals size" in {
    // When slide == size, it behaves like tumbling
    val strategy = WindowStrategy.Sliding(1000, 1000)
    val windows = WindowAssigner.assign(strategy, 500)
    windows should have size 1
    windows.head shouldBe TimeWindow(0, 1000)
  }

  it should "assign event at timestamp 0" in {
    val strategy = WindowStrategy.Sliding(1000, 500)
    val windows = WindowAssigner.assign(strategy, 0)
    windows should not be empty
    windows.foreach { w =>
      w.contains(0) shouldBe true
    }
  }

  it should "assign many windows when slide is small" in {
    val strategy = WindowStrategy.Sliding(1000, 100)
    val windows = WindowAssigner.assign(strategy, 500)
    // Event at 500 should be in windows starting from 0 to 500 (stepping by 100)
    windows.size should be > 1
    windows.foreach { w =>
      w.contains(500) shouldBe true
    }
  }

  "Session window" should "return empty assignment from WindowAssigner" in {
    val strategy = WindowStrategy.Session(500)
    WindowAssigner.assign(strategy, 1000) shouldBe empty
  }

  "Session window tracker" should "merge overlapping sessions" in {
    val tracker = SessionWindowTracker(500)
    // e1 and e2 are within gap, e3 is also within gap of both
    // All three should end up in the same session
    val e1 = Event(1000, "a", Json.fromInt(1))
    val e2 = Event(1400, "a", Json.fromInt(2))  // within 500ms of e1
    val e3 = Event(1300, "a", Json.fromInt(3))  // within 500ms of both, bridges/merges

    tracker.add(e1) shouldBe empty
    tracker.add(e2) shouldBe empty
    tracker.add(e3) shouldBe empty

    val flushed = tracker.flushAll()
    flushed should have size 1
    flushed.head._3 should have size 3
  }

  it should "produce correct window for flushed session" in {
    val tracker = SessionWindowTracker(500)
    tracker.add(Event(1000, "a", Json.fromInt(1)))
    tracker.add(Event(1200, "a", Json.fromInt(2)))
    val flushed = tracker.flushAll()
    val (key, window, events) = flushed.head
    key shouldBe "a"
    window.start shouldBe 1000L
    window.end shouldBe 1201L  // end = max_timestamp + 1
    events should have size 2
  }

  it should "handle flush with no sessions" in {
    val tracker = SessionWindowTracker(500)
    tracker.flushAll() shouldBe empty
  }

  it should "close multiple sessions from different keys independently" in {
    val tracker = SessionWindowTracker(500)
    tracker.add(Event(1000, "a", Json.fromInt(1)))
    tracker.add(Event(1000, "b", Json.fromInt(2)))
    // Add late event to close both
    tracker.add(Event(2000, "a", Json.fromInt(3)))

    val flushed = tracker.flushAll()
    // Should have sessions for both "a" and "b"
    flushed.map(_._1).toSet should contain("b")
  }

  it should "handle rapid consecutive events" in {
    val tracker = SessionWindowTracker(100)
    for i <- 1 to 10 do
      tracker.add(Event(1000 + i, "k", Json.fromInt(i)))

    val flushed = tracker.flushAll()
    flushed should have size 1
    flushed.head._3 should have size 10
  }

  it should "create separate sessions when gap exactly exceeded" in {
    val tracker = SessionWindowTracker(100)
    val e1 = Event(1000, "a", Json.fromInt(1))
    val e2 = Event(1101, "a", Json.fromInt(2))  // gap = 101 > 100
    // Need a third event that is far enough to trigger closing
    val e3 = Event(1500, "a", Json.fromInt(3))  // gap > 100 from e2

    tracker.add(e1)
    tracker.add(e2)
    val closed = tracker.add(e3)
    // e1 should be closed as a separate session
    closed should have size 1
    closed.head._2 should have size 1
  }

  it should "merge multiple existing sessions into one when a bridging event arrives" in {
    val tracker = SessionWindowTracker(500)
    // Build two open sessions for the same key by adding an event well before
    // the first one (so it neither matches nor closes the existing session):
    //   e1 = 1000  -> session [1000,1000]
    //   e2 = 400   -> 400 < 1000-500, no match; 400 > 1000+500 false -> no close
    //                 -> new session [400,400]; both sessions stay open
    //   e3 = 700   -> within gap of BOTH sessions, so matching has >1 entry
    //                 and the merge-additional-sessions branch (indexed.tail) runs
    tracker.add(Event(1000, "a", Json.fromInt(1)))
    tracker.add(Event(400, "a", Json.fromInt(2)))
    tracker.add(Event(700, "a", Json.fromInt(3)))

    val flushed = tracker.flushAll()
    flushed should have size 1
    val (key, window, events) = flushed.head
    key shouldBe "a"
    window.start shouldBe 400L
    window.end shouldBe 1001L
    events should have size 3
    events.map(_.timestamp).toSet shouldBe Set(400L, 700L, 1000L)
  }
