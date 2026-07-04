package stream

import scala.collection.mutable

/** Configuration for a processing pipeline. */
final case class PipelineConfig(
    windowStrategy: WindowStrategy,
    aggregations: Seq[AggregateFunction],
    allowedLatenessMs: Long = 0L
)

/** The main pipeline that wires Source -> Window -> Aggregator -> Sink.
  *
  * For tumbling and sliding windows, events are buffered per (key, window) and
  * emitted once the watermark advances past the window end. For session windows,
  * the SessionWindowTracker handles grouping and gap detection.
  */
final class Pipeline(config: PipelineConfig, source: Source, sink: Sink):

  private val watermark = WatermarkTracker(config.allowedLatenessMs)

  def run(): Unit =
    config.windowStrategy match
      case WindowStrategy.Session(gap) => runSession(gap)
      case _                           => runWindowed()

  // ---- tumbling / sliding ----
  private def runWindowed(): Unit =
    // Buffer: (key, window) -> events
    val buffers = mutable.Map.empty[(String, TimeWindow), mutable.ArrayBuffer[Event]]
    val emitted = mutable.Set.empty[(String, TimeWindow)]

    source.readEvents { event =>
      if watermark.onEvent(event.timestamp) then
        val windows = WindowAssigner.assign(config.windowStrategy, event.timestamp)
        for w <- windows do
          val k = (event.key, w)
          buffers.getOrElseUpdate(k, mutable.ArrayBuffer.empty) += event

        // Emit any windows that the watermark has now passed
        emitReady(buffers, emitted)
    }

    // Flush remaining windows
    for ((k, events) <- buffers if !emitted.contains(k))
      emitResults(k._1, k._2, events.toSeq)

  private def emitReady(
      buffers: mutable.Map[(String, TimeWindow), mutable.ArrayBuffer[Event]],
      emitted: mutable.Set[(String, TimeWindow)]
  ): Unit =
    val wm = watermark.watermark
    for (k @ (key, window), events) <- buffers if window.end <= wm && !emitted.contains(k) do
      emitResults(key, window, events.toSeq)
      emitted += k

  // ---- session ----
  private def runSession(gapMs: Long): Unit =
    val tracker = SessionWindowTracker(gapMs)
    source.readEvents { event =>
      if watermark.onEvent(event.timestamp) then
        val closed = tracker.add(event)
        for (window, events) <- closed do
          emitResults(event.key, window, events)
    }
    // Flush remaining sessions
    for (key, window, events) <- tracker.flushAll() do
      emitResults(key, window, events)

  private def emitResults(key: String, window: TimeWindow, events: Seq[Event]): Unit =
    val results = Aggregator.aggregateAll(events, config.aggregations, key, window)
    results.foreach(sink.write)

object Pipeline:
  /** A demo pipeline configuration: 10-second tumbling windows computing all aggregations. */
  val demoConfig: PipelineConfig = PipelineConfig(
    windowStrategy = WindowStrategy.Tumbling(sizeMs = 10_000),
    aggregations = Seq(
      AggregateFunction.Count,
      AggregateFunction.Sum,
      AggregateFunction.Avg,
      AggregateFunction.Min,
      AggregateFunction.Max
    ),
    allowedLatenessMs = 5_000
  )

  /** Entry point: runs the demo pipeline reading from stdin and writing to stdout. */
  @main def runDemo(): Unit =
    val pipeline = Pipeline(demoConfig, StdinSource, StdoutSink)
    pipeline.run()
