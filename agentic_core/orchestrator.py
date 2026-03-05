from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from .state import MigrationContext


@dataclass
class DecisionContext:
    run_id: str
    project_name: str
    from_step: str
    candidate_steps: List[str]
    step_success: bool
    last_step_error: str = ""
    requires_ddl_upload: bool = False
    requires_human_intervention: bool = False
    human_intervention_reason: str = ""
    self_heal_iteration: int = 0
    max_self_heal_iterations: int = 0
    validation_passed: bool = False
    validation_issue_count: int = 0
    execution_passed: bool = False
    execution_error_count: int = 0
    missing_object_count: int = 0
    retry_count_for_node: int = 0


@dataclass
class OrchestratorDecision:
    from_step: str
    candidate_steps: List[str]
    selected_step: str
    confidence: float
    reason: str
    summary: str
    next_steps: List[str]
    attempt: int
    latency_ms: int
    model: str
    status: str
    error: Optional[str] = None

    def to_payload(self) -> Dict[str, Any]:
        return asdict(self)


def build_decision_context(
    state: MigrationContext,
    from_step: str,
    candidate_steps: List[str],
) -> DecisionContext:
    retry_counts = state.node_retry_counts if isinstance(state.node_retry_counts, dict) else {}
    return DecisionContext(
        run_id=state.session_id,
        project_name=state.project_name,
        from_step=from_step,
        candidate_steps=list(candidate_steps),
        step_success=bool(state.last_step_success),
        last_step_error=state.last_step_error or "",
        requires_ddl_upload=bool(state.requires_ddl_upload),
        requires_human_intervention=bool(state.requires_human_intervention),
        human_intervention_reason=state.human_intervention_reason or "",
        self_heal_iteration=int(state.self_heal_iteration),
        max_self_heal_iterations=int(state.max_self_heal_iterations),
        validation_passed=bool(state.validation_passed),
        validation_issue_count=len(state.validation_issues or []),
        execution_passed=bool(state.execution_passed),
        execution_error_count=len(state.execution_errors or []),
        missing_object_count=len(state.missing_objects or []),
        retry_count_for_node=int(retry_counts.get(from_step, 0)),
    )


class SnowflakeCortexOrchestrator:
    def __init__(
        self,
        *,
        timeout_seconds: int = 15,
        retries: int = 1,
    ) -> None:
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.retries = max(0, int(retries))
        self.model_name = (
            os.getenv("SNOWFLAKE_CORTEX_MODEL")
            or os.getenv("CORTEX_MODEL")
            or "claude-4-sonnet"
        ).strip() or "claude-4-sonnet"
        self.cortex_function = (
            os.getenv("SNOWFLAKE_CORTEX_FUNCTION")
            or "complete"
        ).strip() or "complete"

    def decide(self, state: MigrationContext, context: DecisionContext) -> OrchestratorDecision:
        last_error: Optional[str] = None
        total_attempts = self.retries + 1
        for attempt in range(1, total_attempts + 1):
            started = time.perf_counter()
            try:
                raw = self._invoke_with_timeout(state, context)
                parsed = self._parse_json_response(raw)
                selected = str(parsed.get("next_node") or "").strip()
                confidence = float(parsed.get("confidence", 0))
                reason = str(parsed.get("reason") or "").strip()
                summary = str(parsed.get("summary") or "").strip()
                next_steps = parsed.get("next_steps", [])
                if not isinstance(next_steps, list):
                    raise ValueError("next_steps must be an array")
                next_steps_clean = [str(x).strip() for x in next_steps if str(x).strip()]
                latency_ms = int((time.perf_counter() - started) * 1000)
                return OrchestratorDecision(
                    from_step=context.from_step,
                    candidate_steps=list(context.candidate_steps),
                    selected_step=selected,
                    confidence=max(0.0, min(confidence, 1.0)),
                    reason=reason,
                    summary=summary,
                    next_steps=next_steps_clean,
                    attempt=attempt,
                    latency_ms=latency_ms,
                    model=self.model_name,
                    status="ok",
                    error=None,
                )
            except Exception as exc:
                last_error = str(exc)
                if attempt >= total_attempts:
                    break
        return OrchestratorDecision(
            from_step=context.from_step,
            candidate_steps=list(context.candidate_steps),
            selected_step="human_review",
            confidence=0.0,
            reason="orchestrator_failed",
            summary="Routing decision failed; escalating to human review.",
            next_steps=[],
            attempt=total_attempts,
            latency_ms=0,
            model=self.model_name,
            status="failed",
            error=last_error,
        )

    def _invoke_with_timeout(self, state: MigrationContext, context: DecisionContext) -> str:
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self._invoke_once, state, context)
            try:
                return future.result(timeout=self.timeout_seconds)
            except FuturesTimeoutError as exc:
                future.cancel()
                raise TimeoutError(f"orchestrator timeout after {self.timeout_seconds}s") from exc

    def _invoke_once(self, state: MigrationContext, context: DecisionContext) -> str:
        from langchain_community.chat_models import ChatSnowflakeCortex
        from .integrations import get_snowflake_session

        session = get_snowflake_session(state)
        if session is None:
            raise RuntimeError("snowflake session unavailable for orchestrator")

        prompt = self._build_prompt(context)
        try:
            chat_model = ChatSnowflakeCortex(
                model=self.model_name,
                cortex_function=self.cortex_function,
                session=session,
                temperature=0,
            )
            response = chat_model.invoke(prompt)
            content = getattr(response, "content", response)
            return self._extract_text(content)
        finally:
            try:
                session.close()
            except Exception:
                pass

    def _build_prompt(self, context: DecisionContext) -> str:
        payload = asdict(context)
        return (
            "You are the workflow orchestrator for a SQL migration pipeline.\n"
            "Decide exactly one next node from candidate_steps.\n"
            "You must return ONLY valid JSON (no prose, no markdown, no code fences).\n"
            "JSON schema:\n"
            "{"
            "\"next_node\": string,"
            "\"confidence\": number,"
            "\"reason\": string,"
            "\"summary\": string,"
            "\"next_steps\": string[]"
            "}\n"
            "Rules:\n"
            "1) next_node MUST be one of candidate_steps.\n"
            "2) confidence must be between 0 and 1.\n"
            "3) reason and summary must be concise.\n"
            "4) next_steps should be short actionable bullets.\n"
            f"Decision context:\n{json.dumps(payload, ensure_ascii=False, default=str)}"
        )

    @staticmethod
    def _extract_text(content: Any) -> str:
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts: List[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    text = item.get("text")
                    if isinstance(text, str):
                        parts.append(text)
                    else:
                        parts.append(str(item))
                else:
                    text = getattr(item, "text", None)
                    parts.append(text if isinstance(text, str) else str(item))
            return "\n".join(parts).strip()
        return str(content or "").strip()

    @staticmethod
    def _parse_json_response(raw: str) -> Dict[str, Any]:
        text = str(raw or "").strip()
        if not text:
            raise ValueError("empty orchestrator response")

        if text.startswith("```"):
            lines = text.splitlines()
            if len(lines) >= 2 and lines[-1].startswith("```"):
                text = "\n".join(lines[1:-1]).strip()

        try:
            parsed = json.loads(text)
        except Exception:
            start = text.find("{")
            end = text.rfind("}")
            if start < 0 or end <= start:
                raise ValueError("orchestrator response is not valid JSON")
            parsed = json.loads(text[start : end + 1])

        if not isinstance(parsed, dict):
            raise ValueError("orchestrator response root must be an object")
        return parsed
