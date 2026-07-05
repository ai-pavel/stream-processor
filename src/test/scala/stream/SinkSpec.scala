package stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.Json
import io.circe.parser.*

class SinkSpec extends AnyFlatSpec with Matchers:

  private val window = TimeWindow(0, 1000)

  "CollectorSink" should "collect written results" in {
    val sink = CollectorSink()
    val result = AggregateResult("k", window, AggregateFunction.Count, 5.0)
    sink.write(result)
    sink.results should have size 1
    sink.results.head shouldBe result
  }

  it should "collect multiple results in order" in {
    val sink = CollectorSink()
    val r1 = AggregateResult("k", window, AggregateFunction.Count, 3.0)
    val r2 = AggregateResult("k", window, AggregateFunction.Sum, 15.0)
    val r3 = AggregateResult("k", window, AggregateFunction.Avg, 5.0)
    sink.write(r1)
    sink.write(r2)
    sink.write(r3)
    sink.results shouldBe Seq(r1, r2, r3)
  }

  it should "start empty" in {
    val sink = CollectorSink()
    sink.results shouldBe empty
  }

  it should "clear collected results" in {
    val sink = CollectorSink()
    sink.write(AggregateResult("k", window, AggregateFunction.Count, 1.0))
    sink.results should have size 1
    sink.clear()
    sink.results shouldBe empty
  }

  it should "collect results again after clearing" in {
    val sink = CollectorSink()
    sink.write(AggregateResult("k", window, AggregateFunction.Count, 1.0))
    sink.clear()
    val r = AggregateResult("k2", window, AggregateFunction.Sum, 42.0)
    sink.write(r)
    sink.results shouldBe Seq(r)
  }

  "StdoutSink" should "write JSON to stdout" in {
    val result = AggregateResult("sensor", TimeWindow(0, 1000), AggregateFunction.Sum, 99.5)
    val stream = new java.io.ByteArrayOutputStream()
    Console.withOut(stream) {
      StdoutSink.write(result)
    }
    val output = stream.toString.trim
    val parsed = parse(output)
    parsed.isRight shouldBe true
    val json = parsed.toOption.get
    json.hcursor.downField("key").as[String] shouldBe Right("sensor")
    json.hcursor.downField("value").as[Double] shouldBe Right(99.5)
    json.hcursor.downField("function").as[String] shouldBe Right("Sum")
    json.hcursor.downField("window_start").as[Long] shouldBe Right(0L)
    json.hcursor.downField("window_end").as[Long] shouldBe Right(1000L)
  }

  "Sink trait" should "have a default no-op flush" in {
    // The default flush() in the Sink trait should not throw
    val sink = CollectorSink()
    noException should be thrownBy sink.flush()
  }
