"""Backward-compatible routing import shim."""

from agentic_core.routing.decisions import should_continue, should_continue_after_execute

__all__ = ["should_continue", "should_continue_after_execute"]
