from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
HERMES_SOURCE = Path(
    os.environ.get("HERMES_SOURCE", Path.home() / ".hermes" / "hermes-agent")
).resolve()

if str(HERMES_SOURCE) in sys.path:
    sys.path.remove(str(HERMES_SOURCE))
sys.path.insert(0, str(HERMES_SOURCE))

# Exercise the plugin against the live Hermes checkout and its installed
# runtime dependencies while keeping pytest itself repo-local.
hermes_site_packages = (
    HERMES_SOURCE
    / "venv"
    / "lib"
    / f"python{sys.version_info.major}.{sys.version_info.minor}"
    / "site-packages"
)
if hermes_site_packages.is_dir() and str(hermes_site_packages) not in sys.path:
    sys.path.append(str(hermes_site_packages))

# Prime Hermes' own top-level tools package before the plugin's required
# tools.py module can shadow it when pytest runs from the repository root.
import tools.registry  # noqa: F401,E402


@pytest.fixture
def plugin_package():
    """Load the repository root as a package, like Hermes' plugin loader."""
    package_name = "hermes_g2_bridge_test_plugin"
    for name in list(sys.modules):
        if name == package_name or name.startswith(f"{package_name}."):
            sys.modules.pop(name, None)

    spec = importlib.util.spec_from_file_location(
        package_name,
        REPO_ROOT / "__init__.py",
        submodule_search_locations=[str(REPO_ROOT)],
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[package_name] = module
    spec.loader.exec_module(module)
    yield module

    for name in list(sys.modules):
        if name == package_name or name.startswith(f"{package_name}."):
            sys.modules.pop(name, None)
