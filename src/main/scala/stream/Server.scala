package stream

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax.*

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import scala.collection.mutable

/** A process-lifetime aggregator for server mode.
  *
  * Unlike [[Pipeline]], which reads a finite source to exhaustion and then
  * flushes, this keeps windowed state alive for the lifetime of the process:
  * events are folded in incrementally and the current aggregation state can be
  * observed at any time.
  *
  * Supports the stateless window strategies (tumbling and sliding). All public
  * methods are synchronized because the HTTP server dispatches requests on a
  * thread pool.
  */
final class LiveAggregator(config: PipelineConfig):
  private val watermark = WatermarkTracker(config.allowedLatenessMs)
  private val buffers = mutable.Map.empty[(String, TimeWindow), mutable.ArrayBuffer[Event]]

  /** Fold events into the windowed state. Returns the number of events
    * accepted (events arriving too far behind the watermark are dropped).
    */
  def add(events: Seq[Event]): Int = synchronized {
    var accepted = 0
    for event <- events do
      if watermark.onEvent(event.timestamp) then
        for window <- WindowAssigner.assign(config.windowStrategy, event.timestamp) do
          buffers.getOrElseUpdate((event.key, window), mutable.ArrayBuffer.empty) += event
        accepted += 1
    accepted
  }

  /** Compute the current aggregation results across all buffered windows. */
  def snapshot(): Seq[AggregateResult] = synchronized {
    buffers.toSeq
      .sortBy { case ((key, window), _) => (key, window.start, window.end) }
      .flatMap { case ((key, window), events) =>
        Aggregator.aggregateAll(events.toSeq, config.aggregations, key, window)
      }
  }

/** HTTP server exposing the stream processor as a long-lived microservice.
  *
  * Endpoints:
  *   - GET  /health      -> liveness probe
  *   - POST /events      -> one JSON event or an array of events (stdin format)
  *   - GET  /aggregates  -> current windowed aggregation state as a JSON array
  *
  * Pass port 0 to bind an ephemeral port (useful in tests); the actual port is
  * available via [[boundPort]].
  */
final class StreamServer(config: PipelineConfig = Pipeline.demoConfig, port: Int = 8080):
  private val aggregator = LiveAggregator(config)
  private val server = HttpServer.create(new InetSocketAddress(port), 0)

  server.createContext("/health", handler(handleHealth))
  server.createContext("/events", handler(handleEvents))
  server.createContext("/aggregates", handler(handleAggregates))
  server.setExecutor(Executors.newFixedThreadPool(4))

  def boundPort: Int = server.getAddress.getPort

  def start(): Unit = server.start()

  def stop(): Unit = server.stop(0)

  private def handler(f: HttpExchange => Unit): HttpHandler =
    (exchange: HttpExchange) =>
      try f(exchange)
      catch
        case e: Exception =>
          respond(exchange, 500, Json.obj("error" -> e.toString.asJson).noSpaces)
      finally exchange.close()

  private def handleHealth(exchange: HttpExchange): Unit =
    if exchange.getRequestMethod != "GET" then methodNotAllowed(exchange, "GET")
    else respond(exchange, 200, """{"status":"ok","service":"stream-processor"}""")

  private def handleEvents(exchange: HttpExchange): Unit =
    if exchange.getRequestMethod != "POST" then methodNotAllowed(exchange, "POST")
    else
      val body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
      decode[List[Event]](body).orElse(decode[Event](body).map(List(_))) match
        case Right(events) =>
          val accepted = aggregator.add(events)
          respond(exchange, 202, Json.obj("accepted" -> accepted.asJson).noSpaces)
        case Left(err) =>
          respond(exchange, 400, Json.obj("error" -> err.getMessage.asJson).noSpaces)

  private def handleAggregates(exchange: HttpExchange): Unit =
    if exchange.getRequestMethod != "GET" then methodNotAllowed(exchange, "GET")
    else respond(exchange, 200, aggregator.snapshot().asJson.noSpaces)

  private def methodNotAllowed(exchange: HttpExchange, allowed: String): Unit =
    exchange.getResponseHeaders.set("Allow", allowed)
    respond(exchange, 405, Json.obj("error" -> "method not allowed".asJson).noSpaces)

  private def respond(exchange: HttpExchange, status: Int, body: String): Unit =
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length)
    val os = exchange.getResponseBody
    try os.write(bytes)
    finally os.close()

/** Entry point: serves the demo pipeline configuration over HTTP.
  * Port defaults to 8080 and can be overridden with the PORT env var.
  */
@main def runServer(): Unit =
  val port = sys.env.get("PORT").flatMap(_.toIntOption).getOrElse(8080)
  val server = StreamServer(port = port)
  server.start()
  println(s"stream-processor listening on port ${server.boundPort}")
  Thread.currentThread().join()
