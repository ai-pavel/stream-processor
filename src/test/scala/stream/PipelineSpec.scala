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
