"""Hermes G2 transport and private MCP platform plugin."""

from __future__ import annotations

import os

__all__ = ["register"]


def check_requirements() -> bool:
    """Return whether the deferred adapter dependency is importable."""
    try:
        import websockets  # noqa: F401
    except ImportError:
        return False
    return True


def validate_config(config) -> bool:
    """A non-empty shared token is mandatory for every bind address."""
    del config
    return bool((os.getenv("HERMES_G2_TOKEN") or "").strip())


def register(ctx) -> None:
    """Register the authenticated transport and its tool-free cue model slot.

    Every model-facing workflow belongs to the separately packaged
    ``hermes-g2-workflows`` MCP server. The functions in ``tools.py`` are
    private relay handlers and must never be registered into Hermes' model
    tool inventory by this plugin. The cue slot configures a direct auxiliary
    model call; it does not add an agent-visible tool.
    """
    from .adapter import G2Adapter

    ctx.register_auxiliary_task(
        key="hermes_g2_conversate_cues",
        display_name="G2 Conversate cues",
        description="Fast, tool-free question and topic cues from opt-in recent transcript text.",
        defaults={"provider": "auto", "model": "", "timeout": 2.25},
    )

    ctx.register_platform(
        name="g2",
        label="Even Realities G2",
        adapter_factory=lambda cfg: G2Adapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["HERMES_G2_TOKEN"],
        install_hint="pip install 'websockets>=14,<18'",
        emoji="👓",
        allow_update_command=False,
        max_message_length=16384,
        platform_hint=(
            "The user is speaking through an authenticated Even Realities G2 "
            "smart-glasses connection."
        ),
    )
