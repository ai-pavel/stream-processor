# Stream Processor

[![CI](https://github.com/pavel-genai/stream-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/pavel-genai/stream-processor/actions/workflows/ci.yml)

A Scala 3 real-time event stream processor with windowing, aggregation, and watermark support.

## Features

- **Event model**: Timestamped events with a grouping key and JSON payload
- **Windowing strategies**: Tumbling, sliding, and session windows
- **Aggregation functions**: Count, sum, avg, min, max over windowed events
- **Watermark tracking**: Handles late-arriving events with configurable lateness tolerance
- **Source/Sink abstraction**: Reads JSON lines from stdin, writes aggregated results to stdout

## Project Structure

```
src/main/scala/stream/
  Event.scala       — Event type with JSON codec
  Window.scala      — Window types and assignment strategies
  Aggregator.scala  — Aggregation functions over windowed events
  Watermark.scala   — Watermark tracker for late event handling
  Source.scala       — Source abstraction (stdin, in-memory)
  Sink.scala         — Sink abstraction (stdout, collector)
  Pipeline.scala     — Pipeline wiring and demo configuration
```

## Usage

### Build and run

```bash
sbt compile
sbt run
```

### Input format

Send JSON lines to stdin, one event per line:

```json
{"timestamp":1000,"key":"sensor-1","payload":42.5}
{"timestamp":2000,"key":"sensor-1","payload":38.1}
{"timestamp":11000,"key":"sensor-1","payload":45.0}
```

### Output format

Aggregated results are written as JSON lines to stdout:

```json
{"key":"sensor-1","window_start":0,"window_end":10000,"function":"Count","value":2.0}
{"key":"sensor-1","window_start":0,"window_end":10000,"function":"Sum","value":80.6}
```

### Run tests

```bash
sbt test
```

## Configuration

The demo pipeline uses 10-second tumbling windows with all five aggregation functions and 5 seconds of allowed lateness. Customize by editing `Pipeline.demoConfig` or creating your own `PipelineConfig`.
