import logging
import sys

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from pythonjsonlogger import jsonlogger


class TraceIdFilter(logging.Filter):
    def filter(self, record):
        span = trace.get_current_span()
        sc = span.get_span_context() if span else trace.SpanContext()
        record.trace_id = hex(sc.trace_id) if sc.is_valid else ""
        record.span_id = hex(sc.span_id) if sc.is_valid else ""
        return True


def setup_telemetry(service_name: str, app=None):
    resource = Resource.create(attributes={"service.name": service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter()
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    if app is not None:
        FastAPIInstrumentor.instrument_app(app)

    SQLAlchemyInstrumentor().instrument()

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s trace_id=%(trace_id)s span_id=%(span_id)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    ))
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.addFilter(TraceIdFilter())
