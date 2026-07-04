package stream

import io.circe.*
import io.circe.generic.semiauto.*

/** A timestamped event with a grouping key and an arbitrary JSON payload. */
final case class Event(
    timestamp: Long,
    key: String,
    payload: Json
)

object Event:
  given Decoder[Event] = deriveDecoder[Event]
  given Encoder[Event] = deriveEncoder[Event]

  /** Try to extract a numeric value from the payload for aggregation. */
  def numericValue(event: Event): Option[Double] =
    event.payload.asNumber.flatMap(_.toDouble.some)
      .orElse(event.payload.hcursor.downField("value").as[Double].toOption)

  extension [A](a: A)
    def some: Option[A] = Some(a)
