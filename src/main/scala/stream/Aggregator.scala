package stream

import io.circe.*
import io.circe.syntax.*

/** Supported aggregation functions. */
enum AggregateFunction:
  case Count, Sum, Avg, Min, Max

/** Result of applying an aggregation over a window of events. */
final case class AggregateResult(
    key: String,
    window: TimeWindow,
    function: AggregateFunction,
    value: Double
)

object AggregateResult:
  given Encoder[AggregateResult] = Encoder.instance { r =>
    Json.obj(
      "key" -> r.key.asJson,
      "window_start" -> r.window.start.asJson,
      "window_end" -> r.window.end.asJson,
      "function" -> r.function.toString.asJson,
      "value" -> r.value.asJson
    )
  }

object Aggregator:

  /** Compute a single aggregation over a sequence of events. */
  def aggregate(
      events: Seq[Event],
      function: AggregateFunction,
      key: String,
      window: TimeWindow
  ): AggregateResult =
    val values = events.flatMap(Event.numericValue)
    val result = function match
      case AggregateFunction.Count => events.size.toDouble
      case AggregateFunction.Sum   => values.sum
      case AggregateFunction.Avg   => if values.nonEmpty then values.sum / values.size else 0.0
      case AggregateFunction.Min   => if values.nonEmpty then values.min else 0.0
      case AggregateFunction.Max   => if values.nonEmpty then values.max else 0.0
    AggregateResult(key, window, function, result)

  /** Compute multiple aggregations at once. */
  def aggregateAll(
      events: Seq[Event],
      functions: Seq[AggregateFunction],
      key: String,
      window: TimeWindow
  ): Seq[AggregateResult] =
    functions.map(f => aggregate(events, f, key, window))
