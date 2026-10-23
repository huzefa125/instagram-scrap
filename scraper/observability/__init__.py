"""Tracing: where each Instagram request and activity went, how long it took, and how it ended."""

from .tracing import configure, read_spans, span, summarize

__all__ = ["configure", "read_spans", "span", "summarize"]
