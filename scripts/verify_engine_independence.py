"""Reject DuckDB or Calcite dependencies in the production engine package."""

from __future__ import annotations

import argparse
import ast
import importlib
import importlib.abc
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

FORBIDDEN = frozenset({"calcite", "duckdb"})
STRING_LOADING_CALLS = frozenset(
    {
        "__import__",
        "importlib.import_module",
        "os.popen",
        "os.system",
        "subprocess.call",
        "subprocess.check_call",
        "subprocess.check_output",
        "subprocess.Popen",
        "subprocess.run",
    }
)


@dataclass(frozen=True, order=True)
class Violation:
    path: Path
    line: int
    message: str

    def render(self) -> str:
        return f"{self.path.as_posix()}:{self.line}: {self.message}"


def _forbidden_token(name: str) -> str | None:
    segments = re.split(r"[^a-z0-9]+", name.casefold())
    return next((token for token in FORBIDDEN if token in segments), None)


def _call_name(node: ast.expr) -> str | None:
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    parts.append(node.id)
    return ".".join(reversed(parts))


def scan_file(path: Path) -> list[Violation]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    violations: list[Violation] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                token = _forbidden_token(alias.name)
                if token is not None:
                    violations.append(
                        Violation(path, node.lineno, f"forbidden import: {token}")
                    )
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            token = _forbidden_token(node.module)
            if token is not None:
                violations.append(
                    Violation(path, node.lineno, f"forbidden import: {token}")
                )
        elif isinstance(node, ast.Call):
            call_name = _call_name(node.func)
            token = _forbidden_token(call_name) if call_name is not None else None
            if token is not None:
                violations.append(
                    Violation(path, node.lineno, f"forbidden call: {token}")
                )
            if call_name not in STRING_LOADING_CALLS:
                continue
            for argument in (*node.args, *node.keywords):
                value = argument.value if isinstance(argument, ast.keyword) else argument
                if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
                    continue
                token = _forbidden_token(value.value)
                if token is not None:
                    violations.append(
                        Violation(
                            path,
                            node.lineno,
                            f"forbidden call argument: {token}",
                        )
                    )

    return violations


def static_check(source_root: Path) -> list[Violation]:
    return sorted(
        violation
        for path in source_root.rglob("*.py")
        for violation in scan_file(path)
    )


class ForbiddenImportError(ImportError):
    """Raised when production code attempts to load a forbidden engine."""


class ForbiddenImportFinder(importlib.abc.MetaPathFinder):
    def find_spec(
        self,
        fullname: str,
        path: object = None,
        target: ModuleType | None = None,
    ) -> None:
        del path, target
        token = _forbidden_token(fullname)
        if token is not None:
            raise ForbiddenImportError(f"forbidden runtime import: {token}")
        return None


def runtime_check(source_root: Path) -> list[str]:
    package_name = source_root.name
    parent = str(source_root.parent.resolve())
    module_names: set[str] = set()
    for path in source_root.rglob("*.py"):
        relative = path.relative_to(source_root)
        parts = relative.with_suffix("").parts
        if parts[-1] == "__init__":
            parts = parts[:-1]
        module_names.add(".".join((package_name, *parts)).rstrip("."))

    for name in list(sys.modules):
        if _forbidden_token(name) is not None:
            del sys.modules[name]

    finder = ForbiddenImportFinder()
    sys.meta_path.insert(0, finder)
    sys.path.insert(0, parent)
    failures: list[str] = []
    try:
        for module_name in sorted(module_names):
            try:
                importlib.import_module(module_name)
            except ForbiddenImportError as error:
                failures.append(f"{module_name}: {error}")
            except Exception as error:
                failures.append(
                    f"{module_name}: production module import failed: "
                    f"{type(error).__name__}: {error}"
                )
    finally:
        sys.meta_path.remove(finder)
        sys.path.remove(parent)
    return failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("src/distributed_sql"),
        help="Production package directory to inspect.",
    )
    parser.add_argument(
        "--mode",
        choices=("all", "static", "runtime"),
        default="all",
        help="Checks to execute.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_root = args.source_root.resolve()
    if not (source_root / "__init__.py").is_file():
        print(f"production package not found: {source_root}")
        return 2

    failures: list[str] = []
    if args.mode in {"all", "static"}:
        failures.extend(item.render() for item in static_check(source_root))
    if args.mode in {"all", "runtime"}:
        failures.extend(runtime_check(source_root))

    if failures:
        print("\n".join(failures))
        return 1
    print(f"Engine independence checks passed: {source_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
