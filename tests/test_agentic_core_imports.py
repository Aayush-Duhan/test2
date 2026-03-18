import importlib
import unittest


class ImportSmokeTests(unittest.TestCase):
    def test_canonical_imports_resolve(self):
        modules = [
            "agentic_core",
            "agentic_core.models.context",
            "agentic_core.nodes.init_project",
            "agentic_core.nodes.execute_sql",
            "agentic_core.services.validation",
        ]
        for module_name in modules:
            with self.subTest(module=module_name):
                self.assertIsNotNone(importlib.import_module(module_name))
