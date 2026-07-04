package stream

import io.circe.syntax.*
import io.circe.Encoder

/** Abstraction for writing aggregated results. */
trait Sink:
  def write(result: AggregateResult): Unit
  def flush(): Unit = ()

/** Writes each AggregateResult as a JSON line to stdout. */
object StdoutSink extends Sink:
  override def write(result: AggregateResult): Unit =
    println(result.asJson.noSpaces)

/** Collects results in memory for testing. */
final class CollectorSink extends Sink:
  import scala.collection.mutable
  private val buffer = mutable.ArrayBuffer.empty[AggregateResult]

  override def write(result: AggregateResult): Unit =
    buffer += result

  def results: Seq[AggregateResult] = buffer.toSeq
  def clear(): Unit = buffer.clear()
