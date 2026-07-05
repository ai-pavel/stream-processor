package stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.Json

class PipelineSpec extends AnyFlatSpec with Matchers:

  private def makeEvents(timestamps: Seq[Long], key: String = "k"): Seq[Event] =
    timestamps.map(ts => Event(ts, key, Json.fromDoubleOrNull(ts.toDouble)))

  "Pipeline with tumbling windows" should "aggregate events in correct windows" in {
    val events = makeEvents(Seq(100, 500, 900, 1100, 1500))
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Tumbling(1000),
      aggregations = Seq(AggregateFunction.Count, AggregateFunction.Sum)
    )
    val sink = CollectorSink()
    val pipeline = Pipeline(config, SeqSource(events), sink)
    pipeline.run()

    val counts = sink.results.filter(_.function == AggregateFunction.Count)
    counts should not be empty
    // Window [0, 1000) should have 3 events, [1000, 2000) should have 2
    val w0Count = counts.find(_.window == TimeWindow(0, 1000))
    val w1Count = counts.find(_.window == TimeWindow(1000, 2000))
    w0Count.map(_.value) shouldBe Some(3.0)
    w1Count.map(_.value) shouldBe Some(2.0)
  }

  "Pipeline with sliding windows" should "produce overlapping results" in {
    val events = makeEvents(Seq(250, 750, 1250))
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Sliding(1000, 500),
      aggregations = Seq(AggregateFunction.Count)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()

    // Events should appear in multiple windows
    sink.results.size should be > 1
  }

  "Pipeline with session windows" should "group events by activity" in {
    // Two sessions: [100..300] and [1500] (gap > 500ms)
    val events = makeEvents(Seq(100, 200, 300, 1500))
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Session(500),
      aggregations = Seq(AggregateFunction.Count)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()

    val counts = sink.results.filter(_.function == AggregateFunction.Count)
    counts should have size 2
    counts.map(_.value).toSet shouldBe Set(3.0, 1.0)
  }

  "Pipeline demo config" should "be valid" in {
    val config = Pipeline.demoConfig
    config.windowStrategy shouldBe a[WindowStrategy.Tumbling]
    config.aggregations should have size 5
    config.allowedLatenessMs shouldBe 5000L
  }

  "Pipeline with tumbling windows" should "handle empty source" in {
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Tumbling(1000),
      aggregations = Seq(AggregateFunction.Count)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(Seq.empty), sink).run()
    sink.results shouldBe empty
  }

  it should "handle single event" in {
    val events = makeEvents(Seq(500))
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Tumbling(1000),
      aggregations = Seq(AggregateFunction.Count)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()
    sink.results should have size 1
    sink.results.head.value shouldBe 1.0
  }

  it should "handle multiple keys" in {
    val events = Seq(
      Event(100, "a", Json.fromDoubleOrNull(10.0)),
      Event(200, "b", Json.fromDoubleOrNull(20.0)),
      Event(300, "a", Json.fromDoubleOrNull(30.0)),
      Event(400, "b", Json.fromDoubleOrNull(40.0))
    )
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Tumbling(1000),
      aggregations = Seq(AggregateFunction.Count, AggregateFunction.Sum)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()

    val countsByKey = sink.results.filter(_.function == AggregateFunction.Count)
    countsByKey.find(_.key == "a").map(_.value) shouldBe Some(2.0)
    countsByKey.find(_.key == "b").map(_.value) shouldBe Some(2.0)
  }

  it should "handle events spanning many windows" in {
    val events = makeEvents(Seq(100, 1100, 2100, 3100, 4100))
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Tumbling(1000),
      aggregations = Seq(AggregateFunction.Count)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()
    // Should have a result for each of the 5 windows
    sink.results.filter(_.function == AggregateFunction.Count) should have size 5
  }

  it should "compute all aggregation functions" in {
    val events = makeEvents(Seq(100, 200, 300, 1500))
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Tumbling(1000),
      aggregations = Seq(
        AggregateFunction.Count,
        AggregateFunction.Sum,
        AggregateFunction.Avg,
        AggregateFunction.Min,
        AggregateFunction.Max
      )
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()
    // Window [0, 1000) has 3 events -> 5 aggregations
    // Window [1000, 2000) has 1 event -> 5 aggregations
    sink.results should have size 10
  }

  "Pipeline with late events" should "drop events that are too late" in {
    val events = Seq(
      Event(5000, "k", Json.fromDoubleOrNull(1.0)),
      // watermark = 5000, lateness = 0, so event at 1000 is rejected
      Event(1000, "k", Json.fromDoubleOrNull(2.0)),
      Event(6000, "k", Json.fromDoubleOrNull(3.0))
    )
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Tumbling(10000),
      aggregations = Seq(AggregateFunction.Count),
      allowedLatenessMs = 0
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()
    // Late event at 1000 should be dropped when lateness is 0
    val counts = sink.results.filter(_.function == AggregateFunction.Count)
    // All non-late events are in the same window [0, 10000)
    counts.find(_.window == TimeWindow(0, 10000)).map(_.value) shouldBe Some(2.0)
  }

  it should "accept late events within allowed lateness" in {
    val events = Seq(
      Event(5000, "k", Json.fromDoubleOrNull(1.0)),
      // With lateness = 5000, watermark = 0, event at 3000 is accepted
      Event(3000, "k", Json.fromDoubleOrNull(2.0)),
      Event(6000, "k", Json.fromDoubleOrNull(3.0))
    )
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Tumbling(10000),
      aggregations = Seq(AggregateFunction.Count),
      allowedLatenessMs = 5000
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()
    val counts = sink.results.filter(_.function == AggregateFunction.Count)
    counts.find(_.window == TimeWindow(0, 10000)).map(_.value) shouldBe Some(3.0)
  }

  "Pipeline with sliding windows" should "handle empty source" in {
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Sliding(1000, 500),
      aggregations = Seq(AggregateFunction.Count)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(Seq.empty), sink).run()
    sink.results shouldBe empty
  }

  it should "produce correct counts for overlapping windows" in {
    val events = makeEvents(Seq(250, 750))
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Sliding(1000, 500),
      aggregations = Seq(AggregateFunction.Count)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()
    // Both events in window [0, 1000), event at 750 also in window [500, 1500)
    sink.results should not be empty
  }

  "Pipeline with session windows" should "handle empty source" in {
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Session(500),
      aggregations = Seq(AggregateFunction.Count)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(Seq.empty), sink).run()
    sink.results shouldBe empty
  }

  it should "handle single event as one session" in {
    val events = makeEvents(Seq(1000))
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Session(500),
      aggregations = Seq(AggregateFunction.Count)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()
    val counts = sink.results.filter(_.function == AggregateFunction.Count)
    counts should have size 1
    counts.head.value shouldBe 1.0
  }

  it should "handle multiple keys in session mode" in {
    val events = Seq(
      Event(1000, "a", Json.fromDoubleOrNull(1.0)),
      Event(1100, "b", Json.fromDoubleOrNull(2.0)),
      Event(1200, "a", Json.fromDoubleOrNull(3.0)),
      Event(1300, "b", Json.fromDoubleOrNull(4.0))
    )
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Session(500),
      aggregations = Seq(AggregateFunction.Count)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()
    val counts = sink.results.filter(_.function == AggregateFunction.Count)
    counts should have size 2
    counts.find(_.key == "a").map(_.value) shouldBe Some(2.0)
    counts.find(_.key == "b").map(_.value) shouldBe Some(2.0)
  }

  it should "create multiple sessions with gaps" in {
    // Three separate sessions with gaps > 200ms
    val events = makeEvents(Seq(100, 150, 500, 550, 900, 950))
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Session(200),
      aggregations = Seq(AggregateFunction.Count)
    )
    val sink = CollectorSink()
    Pipeline(config, SeqSource(events), sink).run()
    val counts = sink.results.filter(_.function == AggregateFunction.Count)
    counts should have size 3
    counts.foreach(_.value shouldBe 2.0)
  }

  "PipelineConfig" should "have default allowedLatenessMs of 0" in {
    val config = PipelineConfig(
      windowStrategy = WindowStrategy.Tumbling(1000),
      aggregations = Seq(AggregateFunction.Count)
    )
    config.allowedLatenessMs shouldBe 0L
  }
