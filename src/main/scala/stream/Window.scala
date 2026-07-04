package stream

/** A time window defined by its inclusive start and exclusive end. */
final case class TimeWindow(start: Long, end: Long):
  def contains(ts: Long): Boolean = ts >= start && ts < end
  def durationMs: Long = end - start

/** Windowing strategies that assign events to one or more windows. */
enum WindowStrategy:
  /** Fixed-size, non-overlapping windows. */
  case Tumbling(sizeMs: Long)

  /** Fixed-size windows that advance by a slide interval (may overlap). */
  case Sliding(sizeMs: Long, slideMs: Long)

  /** Windows that close after a gap of inactivity. */
  case Session(gapMs: Long)

object WindowAssigner:

  /** Assign an event to all windows it belongs to under the given strategy.
    * For tumbling and sliding windows the assignment is stateless.
    * Session windows require state and are handled by SessionWindowTracker.
    */
  def assign(strategy: WindowStrategy, timestamp: Long): Seq[TimeWindow] =
    strategy match
      case WindowStrategy.Tumbling(size) =>
        val start = (timestamp / size) * size
        Seq(TimeWindow(start, start + size))

      case WindowStrategy.Sliding(size, slide) =>
        // The event falls into every window whose start <= timestamp < start + size
        val firstStart = (Math.floorDiv(timestamp - size, slide) + 1) * slide
        val starts = Iterator
          .iterate(math.max(0L, firstStart))(_ + slide)
          .takeWhile(s => s <= timestamp)
          .toSeq
        starts.map(s => TimeWindow(s, s + size))

      case WindowStrategy.Session(_) =>
        // Session windows are computed by SessionWindowTracker
        Seq.empty

/** Mutable tracker that merges events into session windows per key. */
final class SessionWindowTracker(gapMs: Long):
  import scala.collection.mutable

  private case class SessionState(var start: Long, var end: Long, events: mutable.ArrayBuffer[Event])

  private val sessions = mutable.Map.empty[String, mutable.ArrayBuffer[SessionState]]

  /** Add an event and return any sessions that have been closed by this event
    * (i.e. sessions whose gap has been exceeded).
    */
  def add(event: Event): Seq[(TimeWindow, Seq[Event])] =
    val key = event.key
    val ts = event.timestamp
    val keySessions = sessions.getOrElseUpdate(key, mutable.ArrayBuffer.empty)

    // Find a session this event extends
    val matching = keySessions.zipWithIndex.filter { case (s, _) =>
      ts >= s.start - gapMs && ts <= s.end + gapMs
    }

    matching match
      case indexed if indexed.isEmpty =>
        keySessions += SessionState(ts, ts, mutable.ArrayBuffer(event))
      case indexed =>
        // Merge into the first matching session
        val (target, _) = indexed.head
        target.start = math.min(target.start, ts)
        target.end = math.max(target.end, ts)
        target.events += event
        // Merge any additional matching sessions into the target
        for (s, idx) <- indexed.tail.sortBy(-_._2) do
          target.start = math.min(target.start, s.start)
          target.end = math.max(target.end, s.end)
          target.events ++= s.events
          keySessions.remove(idx)

    // Emit closed sessions (those whose gap has been exceeded by a newer event)
    val (closed, open) = keySessions.partition(s => ts > s.end + gapMs && s.events.nonEmpty)
    sessions(key) = open
    closed.toSeq.map(s => (TimeWindow(s.start, s.end + 1), s.events.toSeq))

  /** Flush all remaining open sessions. */
  def flushAll(): Seq[(String, TimeWindow, Seq[Event])] =
    val result = for
      (key, keySessions) <- sessions.toSeq
      s <- keySessions
    yield (key, TimeWindow(s.start, s.end + 1), s.events.toSeq)
    sessions.clear()
    result
