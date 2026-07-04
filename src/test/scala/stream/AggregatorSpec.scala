package stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.Json

class AggregatorSpec extends AnyFlatSpec with Matchers:

  private val window = TimeWindow(0, 1000)
  private val events = Seq(
    Event(100, "k", Json.fromDoubleOrNull(10.0)),
    Event(200, "k", Json.fromDoubleOrNull(20.0)),
    Event(300, "k", Json.fromDoubleOrNull(30.0))
  )

  "Aggregator" should "count events" in {
    val r = Aggregator.aggregate(events, AggregateFunction.Count, "k", window)
    r.value shouldBe 3.0
  }

  it should "sum numeric values" in {
    val r = Aggregator.aggregate(events, AggregateFunction.Sum, "k", window)
    r.value shouldBe 60.0
  }

  it should "compute average" in {
    val r = Aggregator.aggregate(events, AggregateFunction.Avg, "k", window)
    r.value shouldBe 20.0
  }

  it should "find minimum" in {
    val r = Aggregator.aggregate(events, AggregateFunction.Min, "k", window)
    r.value shouldBe 10.0
  }

  it should "find maximum" in {
    val r = Aggregator.aggregate(events, AggregateFunction.Max, "k", window)
    r.value shouldBe 30.0
  }

  it should "compute all aggregations at once" in {
    val results = Aggregator.aggregateAll(
      events,
      Seq(AggregateFunction.Count, AggregateFunction.Sum, AggregateFunction.Avg),
      "k",
      window
    )
    results should have size 3
    results.map(_.function) shouldBe Seq(AggregateFunction.Count, AggregateFunction.Sum, AggregateFunction.Avg)
  }

  it should "handle empty events gracefully" in {
    val r = Aggregator.aggregate(Seq.empty, AggregateFunction.Avg, "k", window)
    r.value shouldBe 0.0
  }

  it should "extract value from payload object" in {
    val eventsWithObj = Seq(
      Event(100, "k", Json.obj("value" -> Json.fromDoubleOrNull(5.0))),
      Event(200, "k", Json.obj("value" -> Json.fromDoubleOrNull(15.0)))
    )
    val r = Aggregator.aggregate(eventsWithObj, AggregateFunction.Sum, "k", window)
    r.value shouldBe 20.0
  }
