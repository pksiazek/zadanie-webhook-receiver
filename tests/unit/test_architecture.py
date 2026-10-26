import ast
from pathlib import Path

import webhook_receiver

PACKAGE = Path(webhook_receiver.__file__).parent
FORBIDDEN = {
    "api": {"webhook_receiver.worker"},
    "worker": {"webhook_receiver.api"},
    "common": {"webhook_receiver.api", "webhook_receiver.worker"},
}


def _imports(path: Path, subpackage: str) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(), str(path))):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                # Resolve relative imports against the file's package.
                parts = ["webhook_receiver", subpackage]
                base = parts[: len(parts) - (node.level - 1)]
                name = ".".join(base + ([node.module] if node.module else []))
            else:
                name = node.module or ""
            modules.add(name)
            modules.update(f"{name}.{alias.name}" for alias in node.names)
    return modules


def _violations(subpackage: str) -> list[str]:
    found = []
    for path in sorted((PACKAGE / subpackage).rglob("*.py")):
        for module in _imports(path, subpackage):
            for banned in FORBIDDEN[subpackage]:
                if module == banned or module.startswith(banned + "."):
                    found.append(f"{path.name}: {module}")
    return found


def test_api_and_worker_do_not_import_each_other():
    for subpackage in FORBIDDEN:
        assert list((PACKAGE / subpackage).glob("*.py")), subpackage
        assert _violations(subpackage) == [], subpackage


def test_checker_detects_forbidden_import(tmp_path, monkeypatch):
    fake = tmp_path / "worker"
    fake.mkdir()
    (fake / "bad.py").write_text(
        "from webhook_receiver.api.app import create_app\n"
        "from ..api import schemas\n"
    )
    monkeypatch.setattr("tests.unit.test_architecture.PACKAGE", tmp_path)
    assert sorted(_violations("worker")) == [
        "bad.py: webhook_receiver.api",
        "bad.py: webhook_receiver.api.app",
        "bad.py: webhook_receiver.api.app.create_app",
        "bad.py: webhook_receiver.api.schemas",
    ]
