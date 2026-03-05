"""
LangGraph Core Module for the Autonomous Migration Platform.

This module provides the foundational components for managing the migration
workflow state and orchestrating all migration steps using LangGraph.
"""

# State definitions
from .state import MigrationState, MigrationContext

# Workflow nodes
from .nodes import (
    init_project_node,
    add_source_code_node,
    apply_schema_mapping_node,
    convert_code_node,
    execute_sql_node,
    self_heal_node,
    validate_node,
    human_review_node,
    finalize_node,
)
from .orchestrator import (
    DecisionContext,
    OrchestratorDecision,
    SnowflakeCortexOrchestrator,
    build_decision_context,
)

# Decision function
from .decision import should_continue, should_continue_after_execute

# Integration helpers
from . import integrations

# Public API
__all__ = [
    # State
    "MigrationState",
    "MigrationContext",
    # Nodes
    "init_project_node",
    "add_source_code_node",
    "apply_schema_mapping_node",
    "convert_code_node",
    "execute_sql_node",
    "self_heal_node",
    "validate_node",
    "human_review_node",
    "finalize_node",
    "DecisionContext",
    "OrchestratorDecision",
    "SnowflakeCortexOrchestrator",
    "build_decision_context",
    # Decision
    "should_continue",
    "should_continue_after_execute",
    # Integration
    "integrations",
]

__version__ = "0.1.0"
