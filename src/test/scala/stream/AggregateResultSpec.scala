package stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.syntax.*

class AggregateResultSpec extends AnyFlatSpec with Matchers:

  "AggregateResult" should "encode to JSON with all fields" in {
    val result = AggregateResult("sensor-1", TimeWindow(1000, 2000), AggregateFunction.Sum, 42.5)
    val json = result.asJson
    json.hcursor.downField("key").as[String] shouldBe Right("sensor-1")
    json.hcursor.downField("window_start").as[Long] shouldBe Right(1000L)
    json.hcursor.downField("window_end").as[Long] shouldBe Right(2000L)
    json.hcursor.downField("function").as[String] shouldBe Right("Sum")
    json.hcursor.downField("value").as[Double] shouldBe Right(42.5)
  }

  it should "encode Count function name" in {
    val result = AggregateResult("k", TimeWindow(0, 100), AggregateFunction.Count, 5.0)
    result.asJson.hcursor.downField("function").as[String] shouldBe Right("Count")
  }

  it should "encode Avg function name" in {
    val result = AggregateResult("k", TimeWindow(0, 100), AggregateFunction.Avg, 3.14)
    result.asJson.hcursor.downField("function").as[String] shouldBe Right("Avg")
  }

  it should "encode Min function name" in {
    val result = AggregateResult("k", TimeWindow(0, 100), AggregateFunction.Min, 1.0)
    result.asJson.hcursor.downField("function").as[String] shouldBe Right("Min")
  }

  it should "encode Max function name" in {
    val result = AggregateResult("k", TimeWindow(0, 100), AggregateFunction.Max, 99.0)
    result.asJson.hcursor.downField("function").as[String] shouldBe Right("Max")
  }

  it should "encode zero value" in {
    val result = AggregateResult("k", TimeWindow(0, 100), AggregateFunction.Count, 0.0)
    result.asJson.hcursor.downField("value").as[Double] shouldBe Right(0.0)
  }

  it should "encode negative value" in {
    val result = AggregateResult("k", TimeWindow(0, 100), AggregateFunction.Sum, -15.5)
    result.asJson.hcursor.downField("value").as[Double] shouldBe Right(-15.5)
  }

  it should "produce valid JSON string" in {
    val result = AggregateResult("k", TimeWindow(0, 1000), AggregateFunction.Sum, 42.0)
    val jsonString = result.asJson.noSpaces
    jsonString should include("\"key\":\"k\"")
    jsonString should include("\"window_start\":0")
    jsonString should include("\"window_end\":1000")
    jsonString should include("\"function\":\"Sum\"")
    jsonString should include("\"value\":42")
  }

  "AggregateFunction enum" should "have all expected values" in {
    AggregateFunction.values should contain allOf(
      AggregateFunction.Count,
      AggregateFunction.Sum,
      AggregateFunction.Avg,
      AggregateFunction.Min,
      AggregateFunction.Max
    )
  }

  it should "have exactly 5 values" in {
    AggregateFunction.values should have size 5
  }

  it should "produce correct toString" in {
    AggregateFunction.Count.toString shouldBe "Count"
    AggregateFunction.Sum.toString shouldBe "Sum"
    AggregateFunction.Avg.toString shouldBe "Avg"
    AggregateFunction.Min.toString shouldBe "Min"
    AggregateFunction.Max.toString shouldBe "Max"
  }
