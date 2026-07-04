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
