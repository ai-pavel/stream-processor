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

  it should "return 0.0 for Min with empty events" in {
    val r = Aggregator.aggregate(Seq.empty, AggregateFunction.Min, "k", window)
    r.value shouldBe 0.0
  }

  it should "return 0.0 for Max with empty events" in {
    val r = Aggregator.aggregate(Seq.empty, AggregateFunction.Max, "k", window)
    r.value shouldBe 0.0
  }

  it should "return 0.0 for Sum with empty events" in {
    val r = Aggregator.aggregate(Seq.empty, AggregateFunction.Sum, "k", window)
    r.value shouldBe 0.0
  }

  it should "return 0.0 for Count with empty events" in {
    val r = Aggregator.aggregate(Seq.empty, AggregateFunction.Count, "k", window)
    r.value shouldBe 0.0
  }

  it should "ignore non-numeric payloads for Sum" in {
    val mixedEvents = Seq(
      Event(100, "k", Json.fromDoubleOrNull(10.0)),
      Event(200, "k", Json.fromString("not-a-number")),
      Event(300, "k", Json.fromDoubleOrNull(20.0))
    )
    val r = Aggregator.aggregate(mixedEvents, AggregateFunction.Sum, "k", window)
    r.value shouldBe 30.0
  }

  it should "ignore non-numeric payloads for Avg" in {
    val mixedEvents = Seq(
      Event(100, "k", Json.fromDoubleOrNull(10.0)),
      Event(200, "k", Json.fromString("text")),
      Event(300, "k", Json.fromDoubleOrNull(30.0))
    )
    val r = Aggregator.aggregate(mixedEvents, AggregateFunction.Avg, "k", window)
    // Only 2 numeric values: (10+30)/2 = 20
    r.value shouldBe 20.0
  }

  it should "ignore non-numeric payloads for Min" in {
    val mixedEvents = Seq(
      Event(100, "k", Json.fromDoubleOrNull(10.0)),
      Event(200, "k", Json.Null),
      Event(300, "k", Json.fromDoubleOrNull(5.0))
    )
    val r = Aggregator.aggregate(mixedEvents, AggregateFunction.Min, "k", window)
    r.value shouldBe 5.0
  }

  it should "ignore non-numeric payloads for Max" in {
    val mixedEvents = Seq(
      Event(100, "k", Json.fromDoubleOrNull(10.0)),
      Event(200, "k", Json.arr()),
      Event(300, "k", Json.fromDoubleOrNull(25.0))
    )
    val r = Aggregator.aggregate(mixedEvents, AggregateFunction.Max, "k", window)
    r.value shouldBe 25.0
  }

  it should "count events regardless of payload type" in {
    val mixedEvents = Seq(
      Event(100, "k", Json.fromString("text")),
      Event(200, "k", Json.Null),
      Event(300, "k", Json.fromBoolean(true))
    )
    val r = Aggregator.aggregate(mixedEvents, AggregateFunction.Count, "k", window)
    r.value shouldBe 3.0
  }

  it should "populate AggregateResult fields correctly" in {
    val w = TimeWindow(5000, 10000)
    val r = Aggregator.aggregate(events, AggregateFunction.Sum, "myKey", w)
    r.key shouldBe "myKey"
    r.window shouldBe w
    r.function shouldBe AggregateFunction.Sum
  }

  it should "handle single event" in {
    val singleEvent = Seq(Event(100, "k", Json.fromDoubleOrNull(42.0)))
    val r = Aggregator.aggregate(singleEvent, AggregateFunction.Avg, "k", window)
    r.value shouldBe 42.0
  }

  it should "handle negative values" in {
    val negEvents = Seq(
      Event(100, "k", Json.fromDoubleOrNull(-10.0)),
      Event(200, "k", Json.fromDoubleOrNull(-20.0)),
      Event(300, "k", Json.fromDoubleOrNull(5.0))
    )
    val minR = Aggregator.aggregate(negEvents, AggregateFunction.Min, "k", window)
    minR.value shouldBe -20.0
    val maxR = Aggregator.aggregate(negEvents, AggregateFunction.Max, "k", window)
    maxR.value shouldBe 5.0
    val sumR = Aggregator.aggregate(negEvents, AggregateFunction.Sum, "k", window)
    sumR.value shouldBe -25.0
  }

  it should "aggregateAll with empty functions list" in {
    val results = Aggregator.aggregateAll(events, Seq.empty, "k", window)
    results shouldBe empty
  }

  it should "aggregateAll with single function" in {
    val results = Aggregator.aggregateAll(events, Seq(AggregateFunction.Count), "k", window)
    results should have size 1
    results.head.value shouldBe 3.0
  }
