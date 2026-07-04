package stream

/** Tracks the watermark — the point in event time up to which all events are
  * assumed to have arrived. Events arriving after the watermark has advanced
  * past their window are considered late.
  *
  * @param allowedLatenessMs how far behind the watermark a late event can still be accepted
  */
final class WatermarkTracker(allowedLatenessMs: Long = 0L):
  private var currentWatermark: Long = Long.MinValue
  private var maxTimestamp: Long = Long.MinValue
  private var lateCount: Long = 0

  /** Update the watermark based on a new event timestamp.
    * Returns true if the event is accepted, false if it is too late.
    */
  def onEvent(timestamp: Long): Boolean =
    if timestamp > maxTimestamp then
      maxTimestamp = timestamp
      // Advance watermark to maxTimestamp minus lateness allowance
      currentWatermark = maxTimestamp - allowedLatenessMs
    if timestamp >= currentWatermark - allowedLatenessMs then
      true
    else
      lateCount += 1
      false

  def watermark: Long = currentWatermark
  def lateEventCount: Long = lateCount
