import logging
from typing import Dict, Any, Union
from datetime import datetime

from langgraph.graph import StateGraph, END

from .state import MigrationContext, MigrationState
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
from .decision import should_continue, should_continue_after_execute

# Configure logging
logger = logging.getLogger(__name__)


def build_workflow(entry_point: str = "init_project") -> StateGraph:
    """
    Build the LangGraph StateGraph with all nodes and edges.

    Creates a StateGraph that orchestrates the entire migration workflow:
    1. Initialize project
    2. Add source code
    3. Apply schema mapping
    4. Convert code
    5. Self-heal (loop with validation)
    6. Validate
    7. Human review (if needed)
    8. Finalize

    Returns:
        Configured StateGraph ready for execution
    """
    logger.info("Building LangGraph workflow")

    # Create the StateGraph with MigrationContext as the state type
    workflow = StateGraph(MigrationContext)

    # Add all nodes to the workflow
    workflow.add_node("init_project", init_project_node)
    workflow.add_node("add_source_code", add_source_code_node)
    workflow.add_node("apply_schema_mapping", apply_schema_mapping_node)
    workflow.add_node("convert_code", convert_code_node)
    workflow.add_node("execute_sql", execute_sql_node)
    workflow.add_node("self_heal", self_heal_node)
    workflow.add_node("validate", validate_node)
    workflow.add_node("human_review", human_review_node)
    workflow.add_node("finalize", finalize_node)

    # Set the entry point
    workflow.set_entry_point(entry_point)

    # Define the linear flow from init_project to convert_code
    workflow.add_edge("init_project", "add_source_code")
    workflow.add_edge("add_source_code", "apply_schema_mapping")
    workflow.add_edge("apply_schema_mapping", "convert_code")
    workflow.add_edge("convert_code", "execute_sql")

    workflow.add_conditional_edges(
        "execute_sql",
        should_continue_after_execute,
        {
            "finalize": "finalize",
            "human_review": "human_review",
            "self_heal": "self_heal",
        },
    )

    # Define the self-heal loop: self_heal -> validate
    workflow.add_edge("self_heal", "validate")

    # Define conditional routing after validation
    workflow.add_conditional_edges(
        "validate",
        should_continue,
        {
            "self_heal": "self_heal",
            "human_review": "human_review",
            "finalize": "finalize",
        },
    )

    # Human review is a pause point. Resume is triggered by a fresh run invocation.
    workflow.add_edge("human_review", END)

    # Define the end point
    workflow.add_edge("finalize", END)

    logger.info("LangGraph workflow built successfully")
    return workflow


def run_workflow(initial_state: MigrationContext) -> MigrationContext:
    """
    Run the migration workflow with the given initial state.

    Compiles the workflow and executes it with the provided initial state.

    Args:
        initial_state: Initial migration context with project configuration

    Returns:
        Final migration context after workflow completion
    """
    logger.info(f"Starting workflow for project: {initial_state.project_name}")

    try:
        # Select entrypoint for resume behavior.
        entry_point = "init_project"
        if (
            initial_state.current_stage == MigrationState.HUMAN_REVIEW
            and initial_state.resume_from_stage == "execute_sql"
            and initial_state.ddl_upload_path
        ):
            entry_point = "execute_sql"

        # Build and compile workflow
        workflow = build_workflow(entry_point=entry_point)
        app = workflow.compile()

        # Run workflow
        final_state = app.invoke(initial_state)
        if isinstance(final_state, dict):
            # Coerce dict output to MigrationContext
            if "current_stage" in final_state and isinstance(final_state["current_stage"], str):
                try:
                    final_state["current_stage"] = MigrationState(final_state["current_stage"])
                except Exception:
                    final_state["current_stage"] = MigrationState.ERROR
            if "created_at" in final_state and isinstance(final_state["created_at"], str):
                try:
                    final_state["created_at"] = datetime.fromisoformat(final_state["created_at"])
                except Exception:
                    pass
            if "updated_at" in final_state and isinstance(final_state["updated_at"], str):
                try:
                    final_state["updated_at"] = datetime.fromisoformat(final_state["updated_at"])
                except Exception:
                    pass
            final_state = MigrationContext(**final_state)
        logger.info(f"Workflow completed for project: {initial_state.project_name}")
        logger.info(f"Final state: {final_state.current_stage.value}")
        return final_state
    except Exception as e:
        logger.error(f"Workflow execution failed: {str(e)}")
        initial_state.errors.append(f"Workflow execution failed: {str(e)}")
        initial_state.current_stage = MigrationState.ERROR
        return initial_state