"""Shared result DTOs for agentic_core services."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class SelfHealResult:
    success: bool
    fixed_code: str
    fixes_applied: List[str]
    issues_fixed: int
    error_message: Optional[str] = None
    iteration: int = 0
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class ValidationResult:
    passed: bool
    issues: List[Dict[str, Any]]
    results: Dict[str, Any]
    error_message: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
