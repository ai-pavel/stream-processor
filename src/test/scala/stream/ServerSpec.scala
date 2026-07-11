package stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.Json
import io.circe.parser.parse

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

class ServerSpec extends AnyFlatSpec with Matchers:

  private val client = HttpClient.newHttpClient()

  /** Start a server on an ephemeral port, run the test, and shut it down. */
  private def withServer(config: PipelineConfig = Pipeline.demoConfig)(test: Int => Unit): Unit =
    val server = StreamServer(config, port = 0)
    server.start()
    try test(server.boundPort)
    finally server.stop()

  private def get(port: Int, path: String): HttpResponse[String] =
    val request = HttpRequest.newBuilder(URI.create(s"http://localhost:$port$path")).GET().build()
    client.send(request, HttpResponse.BodyHandlers.ofString())

  private def post(port: Int, path: String, body: String): HttpResponse[String] =
    val request = HttpRequest
      .newBuilder(URI.create(s"http://localhost:$port$path"))
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    client.send(request, HttpResponse.BodyHandlers.ofString())

  "GET /health" should "return the service status" in withServer() { port =>
    val response = get(port, "/health")
    response.statusCode() shouldBe 200
    response.headers().firstValue("Content-Type").orElse("") shouldBe "application/json"
    response.body() shouldBe """{"status":"ok","service":"stream-processor"}"""
  }

  "POST /events" should "accept a single JSON event" in withServer() { port =>
    val response = post(port, "/events", """{"timestamp":100,"key":"k","payload":5.0}""")
    response.statusCode() shouldBe 202
    response.body() shouldBe """{"accepted":1}"""
  }

  it should "accept an array of JSON events" in withServer() { port =>
    val body =
      """[{"timestamp":100,"key":"k","payload":1.0},
        | {"timestamp":200,"key":"k","payload":2.0},
        | {"timestamp":300,"key":"k","payload":3.0}]""".stripMargin
    val response = post(port, "/events", body)
    response.statusCode() shouldBe 202
    response.body() shouldBe """{"accepted":3}"""
  }

  it should "reject malformed JSON" in withServer() { port =>
    val response = post(port, "/events", "not json")
    response.statusCode() shouldBe 400
  }

  it should "not count events dropped as too late" in withServer() { port =>
    // demoConfig allows 5000ms lateness; watermark advances to 100000 - 5000
    post(port, "/events", """{"timestamp":100000,"key":"k","payload":1.0}""").statusCode() shouldBe 202
    val late = post(port, "/events", """{"timestamp":100,"key":"k","payload":2.0}""")
    late.statusCode() shouldBe 202
    late.body() shouldBe """{"accepted":0}"""
  }

  "GET /aggregates" should "return an empty array before any events" in withServer() { port =>
    val response = get(port, "/aggregates")
    response.statusCode() shouldBe 200
    response.body() shouldBe "[]"
  }

  it should "return the current windowed aggregation state" in withServer(
    PipelineConfig(
      windowStrategy = WindowStrategy.Tumbling(1000),
      aggregations = Seq(AggregateFunction.Count, AggregateFunction.Sum)
    )
  ) { port =>
    val body =
      """[{"timestamp":100,"key":"a","payload":10.0},
        | {"timestamp":200,"key":"a","payload":20.0},
        | {"timestamp":1100,"key":"b","payload":5.0}]""".stripMargin
    post(port, "/events", body).statusCode() shouldBe 202

    val response = get(port, "/aggregates")
    response.statusCode() shouldBe 200

    val results = parse(response.body()).flatMap(_.as[List[Json]]).toOption.get
    results should have size 4 // 2 windows x 2 aggregations

    def find(key: String, function: String): Option[Double] =
      results
        .find { j =>
          val c = j.hcursor
          c.get[String]("key").toOption.contains(key) &&
          c.get[String]("function").toOption.contains(function)
        }
        .flatMap(_.hcursor.get[Double]("value").toOption)

    find("a", "Count") shouldBe Some(2.0)
    find("a", "Sum") shouldBe Some(30.0)
    find("b", "Count") shouldBe Some(1.0)
    find("b", "Sum") shouldBe Some(5.0)

    val windowsForA = results.filter(_.hcursor.get[String]("key").toOption.contains("a"))
    windowsForA.foreach { j =>
      j.hcursor.get[Long]("window_start").toOption shouldBe Some(0L)
      j.hcursor.get[Long]("window_end").toOption shouldBe Some(1000L)
    }
  }

  it should "accumulate state across multiple POSTs" in withServer(
    PipelineConfig(
      windowStrategy = WindowStrategy.Tumbling(1000),
      aggregations = Seq(AggregateFunction.Count)
    )
  ) { port =>
    post(port, "/events", """{"timestamp":100,"key":"k","payload":1.0}""")
    post(port, "/events", """{"timestamp":200,"key":"k","payload":2.0}""")

    val results = parse(get(port, "/aggregates").body()).flatMap(_.as[List[Json]]).toOption.get
    results should have size 1
    results.head.hcursor.get[Double]("value").toOption shouldBe Some(2.0)
  }

  "unsupported methods" should "return 405" in withServer() { port =>
    post(port, "/health", "{}").statusCode() shouldBe 405
    get(port, "/events").statusCode() shouldBe 405
    post(port, "/aggregates", "{}").statusCode() shouldBe 405
  }
