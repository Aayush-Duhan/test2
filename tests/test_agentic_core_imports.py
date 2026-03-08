import importlib
import sys
import types
import unittest
from unittest import mock


class ImportSmokeTests(unittest.TestCase):
    def test_canonical_imports_resolve(self):
        modules = [
            "agentic_core",
            "agentic_core.models.context",
            "agentic_core.models.results",
            "agentic_core.routing.decisions",
            "agentic_core.nodes.init_project",
            "agentic_core.nodes.execute_sql",
            "agentic_core.services.validation",
            "agentic_core.services.report_context",
        ]
        for module_name in modules:
            with self.subTest(module=module_name):
                self.assertIsNotNone(importlib.import_module(module_name))

    def test_workflow_import_resolves(self):
        graph_module = types.ModuleType("langgraph.graph")
        graph_module.END = "END"
        graph_module.START = "START"

        class DummyStateGraph:
            def __init__(self, *args, **kwargs):
                pass

        graph_module.StateGraph = DummyStateGraph
        langgraph_module = types.ModuleType("langgraph")
        langgraph_module.graph = graph_module
        pydantic_module = types.ModuleType("pydantic")
        fastapi_module = types.ModuleType("fastapi")

        class DummyBaseModel:
            def __init__(self, *args, **kwargs):
                pass

        class DummyHTTPException(Exception):
            pass

        pydantic_module.BaseModel = DummyBaseModel
        fastapi_module.HTTPException = DummyHTTPException

        with mock.patch.dict(
            sys.modules,
            {
                "langgraph": langgraph_module,
                "langgraph.graph": graph_module,
                "pydantic": pydantic_module,
                "fastapi": fastapi_module,
            },
        ):
            workflow = importlib.import_module("python_execution_service.workflow")
        self.assertIsNotNone(workflow)
