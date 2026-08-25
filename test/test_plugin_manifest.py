from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_manifest_is_transport_only_and_declares_no_model_tools():
    manifest = yaml.safe_load((ROOT / "plugin.yaml").read_text(encoding="utf-8"))

    assert manifest["name"] == "hermes-g2-bridge"
    assert manifest["version"] == "2.1.0"
    assert manifest["manifest_version"] == 1
    assert manifest["kind"] == "platform"
    assert manifest["license"] == "Apache-2.0"
    assert "provides_tools" not in manifest
    assert "model-facing tools, skills, commands" in manifest["description"]
    assert "playwright>=1.50,<2" in manifest["python_dependencies"]
    required_names = {
        item if isinstance(item, str) else item["name"]
        for item in manifest["requires_env"]
    }
    assert required_names == {"HERMES_G2_TOKEN"}
    assert "config_schema" not in manifest


def test_native_plugin_registers_transport_and_tool_free_auxiliary_slot(plugin_package):
    registrations = {"platforms": [], "tools": [], "skills": [], "auxiliary": []}

    class Context:
        def register_platform(self, **kwargs):
            registrations["platforms"].append(kwargs)

        def register_tool(self, **kwargs):
            registrations["tools"].append(kwargs)

        def register_skill(self, name, path, description=""):
            registrations["skills"].append(
                {"name": name, "path": Path(path), "description": description}
            )

        def register_auxiliary_task(self, **kwargs):
            registrations["auxiliary"].append(kwargs)

    plugin_package.register(Context())
    assert [item["name"] for item in registrations["platforms"]] == ["g2"]
    assert registrations["tools"] == []
    assert registrations["skills"] == []
    assert [item["key"] for item in registrations["auxiliary"]] == [
        "hermes_g2_conversate_cues"
    ]
    assert registrations["auxiliary"][0]["defaults"]["timeout"] == 2.25

    hint = registrations["platforms"][0]["platform_hint"]
    assert hint == (
        "The user is speaking through an authenticated Even Realities G2 "
        "smart-glasses connection."
    )
    for forbidden in (
        "g2_",
        "glasses_",
        "operation_id",
        "retry",
        "browser_exec",
        "cron",
        "toolset",
    ):
        assert forbidden not in hint


def test_native_plugin_contains_no_model_prompt_skills():
    skills = ROOT / "skills"
    assert not skills.exists() or not any(skills.rglob("SKILL.md"))


def test_readme_documents_private_mcp_transport_and_portable_workflows():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    normalized = " ".join(readme.split())
    assert "hermes-g2-workflows" in readme
    assert "registers no model-facing tools" in normalized
    assert "Host Session MCP" in readme
    assert "Device MCP" in readme
    assert "Apache-2.0" in readme
    assert "UNLICENSED" not in readme
    for retired in (
        "hermes tools enable g2-reminders",
        "hermes tools enable g2-notify",
        "bundled `hermes-g2-bridge:",
    ):
        assert retired not in readme


def test_public_package_has_apache_license_and_notice():
    license_text = (ROOT / "LICENSE").read_text(encoding="utf-8")
    notice = (ROOT / "NOTICE").read_text(encoding="utf-8")

    assert "Apache License" in license_text
    assert "Version 2.0, January 2004" in license_text
    assert "Copyright 2026 not-benny" in notice


def test_token_is_accepted_only_from_secret_environment(plugin_package, monkeypatch):
    monkeypatch.delenv("HERMES_G2_TOKEN", raising=False)
    config = SimpleNamespace(extra={"token": "must-not-be-read-from-config"})
    assert plugin_package.validate_config(config) is False
