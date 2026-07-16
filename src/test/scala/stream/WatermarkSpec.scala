package stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WatermarkSpec extends AnyFlatSpec with Matchers:

  "WatermarkTracker" should "accept in-order events" in {
    val tracker = WatermarkTracker(0)
    tracker.onEvent(1000) shouldBe true
    tracker.onEvent(2000) shouldBe true
    tracker.onEvent(3000) shouldBe true
  }

  it should "reject late events when lateness is 0" in {
    val tracker = WatermarkTracker(0)
    tracker.onEvent(3000) shouldBe true
    tracker.onEvent(1000) shouldBe false
    tracker.lateEventCount shouldBe 1
  }

  it should "accept late events within allowed lateness" in {
    val tracker = WatermarkTracker(2000)
    tracker.onEvent(5000) shouldBe true
    // Watermark is at 5000 - 2000 = 3000
    // Event at 2000 is within allowed lateness (>= 3000 - 2000 = 1000)
    tracker.onEvent(2000) shouldBe true
  }

  it should "track the watermark correctly" in {
    val tracker = WatermarkTracker(1000)
    tracker.onEvent(5000)
    tracker.watermark shouldBe 4000L
    tracker.onEvent(6000)
    tracker.watermark shouldBe 5000L
  }

  it should "start with Long.MinValue watermark" in {
    val tracker = WatermarkTracker(0)
    tracker.watermark shouldBe Long.MinValue
  }

  it should "start with zero late event count" in {
    val tracker = WatermarkTracker(0)
    tracker.lateEventCount shouldBe 0
  }

  it should "accept the first event regardless" in {
    val tracker = WatermarkTracker(0)
    tracker.onEvent(0) shouldBe true
    tracker.watermark shouldBe 0L
  }

  it should "accept events with same timestamp" in {
    val tracker = WatermarkTracker(0)
    tracker.onEvent(1000) shouldBe true
    tracker.onEvent(1000) shouldBe true
    tracker.lateEventCount shouldBe 0
  }

  it should "not advance watermark for older events" in {
    val tracker = WatermarkTracker(0)
    tracker.onEvent(5000) shouldBe true
    tracker.watermark shouldBe 5000L
    tracker.onEvent(3000) // late, rejected
    tracker.watermark shouldBe 5000L // watermark should not change
  }

  it should "count multiple late events" in {
    val tracker = WatermarkTracker(0)
    tracker.onEvent(5000)
    tracker.onEvent(1000) // late
    tracker.onEvent(2000) // late
    tracker.onEvent(500)  // late
    tracker.lateEventCount shouldBe 3
  }

  it should "reject events just below the lateness threshold" in {
    val tracker = WatermarkTracker(1000)
    tracker.onEvent(5000) // watermark = 4000
    // Events below 4000 - 1000 = 3000 should be rejected
    tracker.onEvent(2999) shouldBe false
    tracker.lateEventCount shouldBe 1
  }

  it should "accept events exactly at the lateness boundary" in {
    val tracker = WatermarkTracker(1000)
    tracker.onEvent(5000) // watermark = 4000
    // Events at 4000 - 1000 = 3000 should be accepted
    tracker.onEvent(3000) shouldBe true
    tracker.lateEventCount shouldBe 0
  }

  it should "handle large timestamp values" in {
    val tracker = WatermarkTracker(0)
    val largeTs = Long.MaxValue - 1000
    tracker.onEvent(largeTs) shouldBe true
    tracker.watermark shouldBe largeTs
  }

  it should "handle zero lateness with monotonically increasing events" in {
    val tracker = WatermarkTracker(0)
    for ts <- 1000L to 5000L by 1000L do
      tracker.onEvent(ts) shouldBe true
    tracker.watermark shouldBe 5000L
    tracker.lateEventCount shouldBe 0
  }

  it should "default allowedLatenessMs to 0 when constructed with no args" in {
    val tracker = WatermarkTracker()
    tracker.onEvent(1000) shouldBe true
    // lateness 0: an older event is rejected
    tracker.onEvent(500) shouldBe false
    tracker.lateEventCount shouldBe 1
  }
