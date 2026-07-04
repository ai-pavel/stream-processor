package stream

import io.circe.parser.decode
import scala.io.StdIn

/** Abstraction for reading events from an input stream. */
trait Source:
  /** Read events one at a time, invoking the callback for each successfully parsed event.
    * Returns when the source is exhausted.
    */
  def readEvents(handler: Event => Unit): Unit

/** Reads JSON-line events from stdin. Each line should be a JSON object
  * with fields: timestamp (Long), key (String), payload (JSON value).
  */
object StdinSource extends Source:
  override def readEvents(handler: Event => Unit): Unit =
    Iterator
      .continually(StdIn.readLine())
      .takeWhile(_ != null)
      .foreach { line =>
        if line.trim.nonEmpty then
          decode[Event](line) match
            case Right(event) => handler(event)
            case Left(err)    => System.err.println(s"Parse error: ${err.getMessage}")
      }

/** A source backed by an in-memory sequence, useful for testing. */
final class SeqSource(events: Seq[Event]) extends Source:
  override def readEvents(handler: Event => Unit): Unit =
    events.foreach(handler)
