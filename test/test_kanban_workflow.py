from __future__ import annotations

import asyncio
import importlib
import json
import os
import shutil
import sqlite3
import stat
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


_OPERATION_ID = "kanban." + "1" * 32


def _active_adapter(authorization):
    class Adapter:
        def authorize_active_g2_turn(self):
            return authorization

    return Adapter()


class _RevocableAdapter:
    def __init__(self, authorization):
        self.authorization = authorization
        self.active = True

    def authorize_active_g2_turn(self):
        if not self.active:
            raise PermissionError("turn revoked")
        return self.authorization


def _authorize_handler(tools, monkeypatch, adapter):
    async def authorize(expected=None):
        assert expected is None
        return adapter.authorize_active_g2_turn()

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    monkeypatch.setattr(tools.runtime, "get_active", lambda: adapter)


def _prepare_board(monkeypatch, tmp_path, *, slug="hermes-g2", name="Hermes G2"):
    kanban_home = tmp_path / "shared-kanban"
    profile_home = tmp_path / "profile"
    kanban_home.mkdir()
    profile_home.mkdir()
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(kanban_home))
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    monkeypatch.delenv("HERMES_KANBAN_DB", raising=False)
    from hermes_cli import kanban_db as kb

    kb.create_board(slug, name=name)
    return kb


def _prepare_fresh_default(monkeypatch, tmp_path):
    kanban_home = tmp_path / "fresh-shared-kanban"
    profile_home = tmp_path / "fresh-profile"
    kanban_home.mkdir()
    profile_home.mkdir()
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(kanban_home))
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    monkeypatch.delenv("HERMES_KANBAN_DB", raising=False)
    from hermes_cli import kanban_db as kb

    return kb


@pytest.mark.asyncio
async def test_exact_board_create_is_blocked_unassigned_and_idempotent(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    authorization = object()

    async def authorize(expected=None):
        assert expected is None
        return authorization

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    adapter = _active_adapter(authorization)
    monkeypatch.setattr(
        tools.runtime, "get_active", lambda: adapter
    )
    monkeypatch.setenv("HERMES_SESSION_ID", "session-g2-kanban-test")
    payload = {
        "operation_id": _OPERATION_ID,
        "title": "Mimecast creation",
        "body": "Waiting for account owner input",
        "board": "Hermes G2",
    }

    first = json.loads(await tools._handle_kanban_task_create(payload))
    second = json.loads(await tools._handle_kanban_task_create(payload))
    changed = json.loads(await tools._handle_kanban_task_create({
        **payload,
        "title": "A changed payload must not reuse the receipt",
    }))

    assert first["success"] is True, first
    assert first["receipt"] == {
        "status": "acknowledged",
        "operation_id": _OPERATION_ID,
        "task_id": first["receipt"]["task_id"],
        "created_status": "blocked",
        "created_assignee": None,
        "board": "hermes-g2",
    }
    assert second["receipt"] == {
        **first["receipt"],
        "status": "historical_acknowledgement",
    }
    assert changed == {
        "success": False,
        "commit_state": "committed",
        "operation_id": _OPERATION_ID,
        "error_code": "operation_conflict",
        "error": (
            "Kanban operation identity is already bound to different arguments"
        ),
    }
    with kb.connect_closing(board="hermes-g2") as conn:
        tasks = kb.list_tasks(conn, include_archived=True)
    assert len(tasks) == 1
    task = tasks[0]
    assert task.id == first["receipt"]["task_id"]
    assert task.title == payload["title"]
    assert task.body == payload["body"]
    assert task.status == "blocked"
    assert task.assignee is None
    assert task.created_by == tools._KANBAN_CREATED_BY
    assert task.session_id == "session-g2-kanban-test"
    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.recompute_ready(conn) == 0
        parked = kb.get_task(conn, task.id)
        assert parked is not None
        assert parked.status == "blocked"
        assert kb._has_sticky_block(conn, task.id) is True


@pytest.mark.asyncio
async def test_fresh_canonical_default_board_supports_the_first_card(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_fresh_default(monkeypatch, tmp_path)
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    assert kb.list_boards(include_archived=False)[0]["slug"] == "default"
    assert not kb.kanban_db_path("default").exists()

    result = json.loads(await tools._handle_kanban_task_create({
        "operation_id": _OPERATION_ID,
        "title": "First default card",
        "board": "Default",
    }))

    assert result["success"] is True
    assert result["receipt"]["board"] == "default"
    assert kb.kanban_db_path("default").is_file()
    with kb.connect_closing(board="default") as conn:
        tasks = kb.list_tasks(conn, include_archived=True)
    assert [task.id for task in tasks] == [result["receipt"]["task_id"]]
    assert tasks[0].status == "blocked"
    assert tasks[0].assignee is None


def test_serialized_canonical_create_prevents_concurrent_duplicates(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)

    def create():
        return tools._execute_kanban_task_create(
            kb,
            operation_id=_OPERATION_ID,
            title="Only once",
            body=None,
            board_input="hermes-g2",
            session_id="session-concurrent",
            reauthorize=lambda: None,
            cancelled=threading.Event(),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(create)
        second_future = executor.submit(create)
        receipts = [first_future.result(), second_future.result()]

    assert {receipt["task_id"] for receipt in receipts} == {
        receipts[0]["task_id"]
    }
    assert {receipt["status"] for receipt in receipts} == {
        "acknowledged",
        "historical_acknowledgement",
    }
    with kb.connect_closing(board="hermes-g2") as conn:
        tasks = kb.list_tasks(conn, include_archived=True)
    assert len(tasks) == 1
    assert tasks[0].status == "blocked"


@pytest.mark.asyncio
async def test_missing_or_ambiguous_board_returns_choices_without_mutation(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    kb.create_board("team-one", name="Operations")
    kb.create_board("team-two", name="Operations")
    authorization = object()

    async def authorize(expected=None):
        assert expected is None
        return authorization

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    adapter = _active_adapter(authorization)
    monkeypatch.setattr(
        tools.runtime, "get_active", lambda: adapter
    )

    for requested, error_code in (
        ("Blocker", "board_not_found"),
        ("blocked", "board_not_found"),
        ("Operations", "board_ambiguous"),
    ):
        result = json.loads(await tools._handle_kanban_task_create({
            "operation_id": _OPERATION_ID,
            "title": "Must not be written",
            "board": requested,
        }))
        assert result["success"] is False
        assert result["commit_state"] == "not_committed"
        assert result["error_code"] == error_code
        assert result["boards_truncated"] is False
        assert result["available_boards"] == [
            {"slug": "default", "name": "Default"},
            {"slug": "hermes-g2", "name": "Hermes G2"},
            {"slug": "team-one", "name": "Operations"},
            {"slug": "team-two", "name": "Operations"},
        ]

    for board in ("default", "hermes-g2", "team-one", "team-two"):
        with kb.connect_closing(board=board) as conn:
            assert kb.list_tasks(conn, include_archived=True) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "requested",
    ["Missing private board", "Secret Board Name"],
)
async def test_revocation_after_board_listing_never_leaks_private_choices(
    plugin_package, monkeypatch, tmp_path, requested
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    kb.create_board("secret-one", name="Secret Board Name")
    kb.create_board("secret-two", name="Secret Board Name")
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    original_list = tools._canonical_kanban_boards

    def list_then_revoke(canonical_kb):
        projection = original_list(canonical_kb)
        adapter.active = False
        return projection

    monkeypatch.setattr(tools, "_canonical_kanban_boards", list_then_revoke)
    result = json.loads(await tools._handle_kanban_task_create({
        "operation_id": _OPERATION_ID,
        "title": "Must not leak inventory",
        "board": requested,
    }))

    assert result == {
        "success": False,
        "commit_state": "not_committed",
        "error": "G2 turn authority expired before Kanban creation",
    }
    assert "available_boards" not in result
    for board in ("default", "hermes-g2", "secret-one", "secret-two"):
        with kb.connect_closing(board=board) as conn:
            assert kb.list_tasks(conn, include_archived=True) == []
    ledger_path = Path(os.environ["HERMES_HOME"]) / "state" / (
        "g2-workflows/kanban-operations.sqlite3"
    )
    with sqlite3.connect(ledger_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_revocation_after_generation_never_writes_prepared_intent(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    original_generation = tools._kanban_board_generation

    def generate_then_revoke(*args, **kwargs):
        generation = original_generation(*args, **kwargs)
        adapter.active = False
        return generation

    monkeypatch.setattr(
        tools, "_kanban_board_generation", generate_then_revoke
    )
    result = json.loads(await tools._handle_kanban_task_create({
        "operation_id": _OPERATION_ID,
        "title": "Must not persist stale intent",
        "board": "Hermes G2",
    }))

    assert result == {
        "success": False,
        "commit_state": "not_committed",
        "error": "G2 turn authority expired before Kanban creation",
    }
    ledger_path = Path(os.environ["HERMES_HOME"]) / "state" / (
        "g2-workflows/kanban-operations.sqlite3"
    )
    with sqlite3.connect(ledger_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0
    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []


def test_board_choices_are_bounded_without_limiting_exact_resolution(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    for index in range(20):
        kb.create_board(f"board-{index:02d}", name=f"Board {index:02d}")

    matching, available, truncated = tools._canonical_kanban_boards(kb)

    assert len(matching) == 22
    assert len(available) == tools._KANBAN_BOARD_LIST_LIMIT
    assert truncated is True
    assert tools._resolve_exact_kanban_board("Board 19", matching) == (
        "board-19",
        None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"operation_id": _OPERATION_ID, "title": "Task"},
        {"operation_id": _OPERATION_ID, "board": "Hermes G2"},
        {"operation_id": "kanban.bad", "title": "Task", "board": "Hermes G2"},
        {"operation_id": _OPERATION_ID, "title": "line\nbreak", "board": "Hermes G2"},
        {"operation_id": _OPERATION_ID, "title": "Task", "body": "line\nbreak", "board": "Hermes G2"},
        {"operation_id": _OPERATION_ID, "title": "Task", "board": "x" * 81},
        {"operation_id": _OPERATION_ID, "title": "Task", "board": "Hermes G2", "tool": "kanban_create"},
    ],
)
async def test_invalid_or_generic_kanban_payload_never_reaches_authority_or_db(
    plugin_package, monkeypatch, payload
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    calls = []

    async def authorize(*_args, **_kwargs):
        calls.append("authorize")
        raise AssertionError("invalid payload reached authority")

    monkeypatch.setattr(tools, "_current_session_platform", lambda: "g2")
    monkeypatch.setattr(tools, "_authorize_active_g2_read", authorize)
    result = json.loads(await tools._handle_kanban_task_create(payload))

    assert result["success"] is False
    assert result["commit_state"] == "not_committed"
    assert calls == []


@pytest.mark.asyncio
async def test_committed_tombstone_survives_assignment_archive_and_hard_delete(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    authorization = object()
    adapter = _RevocableAdapter(authorization)
    _authorize_handler(tools, monkeypatch, adapter)
    payload = {
        "operation_id": _OPERATION_ID,
        "title": "Persistent receipt",
        "body": "Immutable creation facts only",
        "board": "Hermes G2",
    }

    first = json.loads(await tools._handle_kanban_task_create(payload))
    task_id = first["receipt"]["task_id"]
    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.assign_task(conn, task_id, "even-g2") is True
    assigned_retry = json.loads(await tools._handle_kanban_task_create(payload))
    assert assigned_retry["receipt"] == {
        **first["receipt"],
        "status": "historical_acknowledgement",
    }
    assert "task_status" not in assigned_retry["receipt"]
    assert assigned_retry["receipt"]["created_status"] == "blocked"
    assert assigned_retry["receipt"]["created_assignee"] is None

    with kb.connect_closing(board="hermes-g2") as conn:
        current = kb.get_task(conn, task_id)
        assert current is not None and current.assignee == "even-g2"
        assert kb.archive_task(conn, task_id) is True
    archived_retry = json.loads(await tools._handle_kanban_task_create(payload))
    assert archived_retry["receipt"] == assigned_retry["receipt"]

    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.delete_archived_task(conn, task_id) is True
    deleted_retry = json.loads(await tools._handle_kanban_task_create(payload))
    assert deleted_retry["receipt"] == assigned_retry["receipt"]
    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []


@pytest.mark.asyncio
async def test_committed_tombstone_never_reroutes_after_display_name_reuse(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    payload = {
        "operation_id": _OPERATION_ID,
        "title": "Original board only",
        "board": "Hermes G2",
    }

    first = json.loads(await tools._handle_kanban_task_create(payload))
    kb.write_board_metadata("hermes-g2", name="Renamed original")
    kb.create_board("replacement", name="Hermes G2")
    retry = json.loads(await tools._handle_kanban_task_create(payload))

    assert retry["receipt"] == {
        **first["receipt"],
        "status": "historical_acknowledgement",
    }
    assert retry["receipt"]["board"] == "hermes-g2"
    with kb.connect_closing(board="replacement") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []


@pytest.mark.asyncio
async def test_committed_tombstone_never_recreates_on_reused_slug(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    payload = {
        "operation_id": _OPERATION_ID,
        "title": "Original board generation",
        "board": "hermes-g2",
    }

    first = json.loads(await tools._handle_kanban_task_create(payload))
    shutil.rmtree(kb.board_dir("hermes-g2"))
    kb.create_board("hermes-g2", name="Hermes G2")
    retry = json.loads(await tools._handle_kanban_task_create(payload))

    assert retry["receipt"] == {
        **first["receipt"],
        "status": "historical_acknowledgement",
    }
    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []


def test_ledger_stores_digest_only_with_owner_only_permissions(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    title = "Do not duplicate this private title"
    body = "Nor persist this private body in a second plaintext store"
    receipt = tools._execute_kanban_task_create(
        kb,
        operation_id=_OPERATION_ID,
        title=title,
        body=body,
        board_input="Hermes G2",
        session_id=None,
        reauthorize=lambda: None,
        cancelled=threading.Event(),
    )
    assert receipt["status"] == "acknowledged"

    ledger_dir = Path(os.environ["HERMES_HOME"]) / "state" / "g2-workflows"
    db_path = ledger_dir / "kanban-operations.sqlite3"
    lock_path = ledger_dir / "kanban-operations.lock"
    assert stat.S_IMODE(ledger_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(db_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(lock_path.stat().st_mode) == 0o600
    raw = db_path.read_bytes()
    assert title.encode() not in raw
    assert body.encode() not in raw
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT payload_digest, state, task_id, created_status, "
            "created_assignee FROM operations WHERE operation_id = ?",
            (_OPERATION_ID,),
        ).fetchone()
    assert row is not None
    assert row[0] == tools._kanban_payload_digest(
        title=title, body=body, board_input="Hermes G2"
    )
    assert row[1:] == ("COMMITTED", receipt["task_id"], "blocked", None)


def _prepare_ledger_state(
    tools,
    kb,
    *,
    title="Recoverable",
    body=None,
    board_input="Hermes G2",
    mutating=False,
):
    generation = tools._kanban_board_generation(kb, "hermes-g2")
    digest = tools._kanban_payload_digest(
        title=title, body=body, board_input=board_input
    )
    with tools._kanban_operation_lock(threading.Event()) as ledger_dir:
        with tools.contextlib.closing(
            tools._connect_kanban_ledger(ledger_dir)
        ) as ledger:
            tools._insert_kanban_intent(
                ledger,
                operation_id=_OPERATION_ID,
                payload_digest=digest,
                generation=generation,
            )
            if mutating:
                tools._mark_kanban_mutating(ledger, _OPERATION_ID)
    return generation


def test_prepared_intent_resumes_original_generation_once(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    _prepare_ledger_state(tools, kb)

    receipt = tools._execute_kanban_task_create(
        kb,
        operation_id=_OPERATION_ID,
        title="Recoverable",
        body=None,
        board_input="Hermes G2",
        session_id=None,
        reauthorize=lambda: None,
        cancelled=threading.Event(),
    )

    assert receipt["status"] == "acknowledged"
    with kb.connect_closing(board="hermes-g2") as conn:
        tasks = kb.list_tasks(conn, include_archived=True)
    assert [task.id for task in tasks] == [receipt["task_id"]]


def test_prepared_intent_does_not_reresolve_reused_display_name(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    _prepare_ledger_state(tools, kb)
    kb.write_board_metadata("hermes-g2", name="Original renamed")
    kb.create_board("replacement", name="Hermes G2")

    receipt = tools._execute_kanban_task_create(
        kb,
        operation_id=_OPERATION_ID,
        title="Recoverable",
        body=None,
        board_input="Hermes G2",
        session_id=None,
        reauthorize=lambda: None,
        cancelled=threading.Event(),
    )

    assert receipt["board"] == "hermes-g2"
    with kb.connect_closing(board="hermes-g2") as conn:
        assert [task.id for task in kb.list_tasks(conn)] == [receipt["task_id"]]
    with kb.connect_closing(board="replacement") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []


def test_mutating_without_exact_row_is_permanently_outcome_unknown(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    _prepare_ledger_state(tools, kb, mutating=True)

    for _attempt in range(2):
        with pytest.raises(tools._KanbanOperationError) as raised:
            tools._execute_kanban_task_create(
                kb,
                operation_id=_OPERATION_ID,
                title="Recoverable",
                body=None,
                board_input="Hermes G2",
                session_id=None,
                reauthorize=lambda: None,
                cancelled=threading.Event(),
            )
        assert raised.value.error_code == "operation_outcome_unknown"
        assert raised.value.commit_state == "unknown"
    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []


def test_mutating_recovers_one_exact_existing_canonical_row(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    _prepare_ledger_state(tools, kb, mutating=True)
    key = tools._KANBAN_IDEMPOTENCY_PREFIX + _OPERATION_ID
    with kb.connect_closing(board="hermes-g2") as conn:
        with kb.write_txn(conn):
            task_id = kb.create_task(
                conn,
                title="Recoverable",
                body=None,
                assignee=None,
                created_by=tools._KANBAN_CREATED_BY,
                workspace_kind="scratch",
                triage=False,
                idempotency_key=key,
                initial_status="blocked",
                board="hermes-g2",
            )
            kb._append_event(
                conn,
                task_id,
                "blocked",
                {
                    "reason": "parked by explicit G2 Kanban creation",
                    "kind": "needs_input",
                    "source_status": "ready",
                },
            )

    receipt = tools._execute_kanban_task_create(
        kb,
        operation_id=_OPERATION_ID,
        title="Recoverable",
        body=None,
        board_input="Hermes G2",
        session_id=None,
        reauthorize=lambda: None,
        cancelled=threading.Event(),
    )
    assert receipt["status"] == "historical_acknowledgement"
    assert receipt["task_id"] == task_id


@pytest.mark.parametrize("later_change", ["assigned", "archived"])
def test_mutating_recovery_never_claims_changed_current_task_state(
    plugin_package, monkeypatch, tmp_path, later_change
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    _prepare_ledger_state(tools, kb, mutating=True)
    key = tools._KANBAN_IDEMPOTENCY_PREFIX + _OPERATION_ID
    with kb.connect_closing(board="hermes-g2") as conn:
        with kb.write_txn(conn):
            task_id = kb.create_task(
                conn,
                title="Recoverable",
                body=None,
                assignee=None,
                created_by=tools._KANBAN_CREATED_BY,
                workspace_kind="scratch",
                triage=False,
                idempotency_key=key,
                initial_status="blocked",
                board="hermes-g2",
            )
            kb._append_event(
                conn,
                task_id,
                "blocked",
                {
                    "reason": "parked by explicit G2 Kanban creation",
                    "kind": "needs_input",
                    "source_status": "ready",
                },
            )
        if later_change == "assigned":
            assert kb.assign_task(conn, task_id, "even-g2") is True
        else:
            assert kb.archive_task(conn, task_id) is True

    with pytest.raises(tools._KanbanOperationError) as raised:
        tools._execute_kanban_task_create(
            kb,
            operation_id=_OPERATION_ID,
            title="Recoverable",
            body=None,
            board_input="Hermes G2",
            session_id=None,
            reauthorize=lambda: None,
            cancelled=threading.Event(),
        )
    assert raised.value.error_code == "operation_outcome_unknown"
    assert raised.value.commit_state == "unknown"


def test_crash_after_mutating_before_board_open_never_recreates(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    original_open = tools._open_existing_kanban_board

    def crash_before_open(_generation):
        raise RuntimeError("simulated process death before board open")

    monkeypatch.setattr(tools, "_open_existing_kanban_board", crash_before_open)
    with pytest.raises(tools._KanbanOperationError) as first_failure:
        tools._execute_kanban_task_create(
            kb,
            operation_id=_OPERATION_ID,
            title="Never resurrect",
            body=None,
            board_input="Hermes G2",
            session_id=None,
            reauthorize=lambda: None,
            cancelled=threading.Event(),
        )
    assert first_failure.value.commit_state == "unknown"
    monkeypatch.setattr(tools, "_open_existing_kanban_board", original_open)

    with pytest.raises(tools._KanbanOperationError) as retry_failure:
        tools._execute_kanban_task_create(
            kb,
            operation_id=_OPERATION_ID,
            title="Never resurrect",
            body=None,
            board_input="Hermes G2",
            session_id=None,
            reauthorize=lambda: None,
            cancelled=threading.Event(),
        )
    assert retry_failure.value.error_code == "operation_outcome_unknown"
    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []


def test_crash_after_board_commit_recovers_exact_row_without_duplicate(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    original_finalize = tools._finalize_kanban_ledger

    def crash_before_finalize(*_args, **_kwargs):
        raise RuntimeError("simulated process death before ledger finalize")

    monkeypatch.setattr(tools, "_finalize_kanban_ledger", crash_before_finalize)
    with pytest.raises(RuntimeError):
        tools._execute_kanban_task_create(
            kb,
            operation_id=_OPERATION_ID,
            title="Recover committed row",
            body=None,
            board_input="Hermes G2",
            session_id=None,
            reauthorize=lambda: None,
            cancelled=threading.Event(),
        )
    with kb.connect_closing(board="hermes-g2") as conn:
        before_retry = kb.list_tasks(conn, include_archived=True)
    assert len(before_retry) == 1
    monkeypatch.setattr(tools, "_finalize_kanban_ledger", original_finalize)

    receipt = tools._execute_kanban_task_create(
        kb,
        operation_id=_OPERATION_ID,
        title="Recover committed row",
        body=None,
        board_input="Hermes G2",
        session_id=None,
        reauthorize=lambda: None,
        cancelled=threading.Event(),
    )
    assert receipt["status"] == "historical_acknowledgement"
    assert receipt["task_id"] == before_retry[0].id
    with kb.connect_closing(board="hermes-g2") as conn:
        after_retry = kb.list_tasks(conn, include_archived=True)
    assert [task.id for task in after_retry] == [before_retry[0].id]


def test_crash_after_board_commit_then_hard_delete_stays_unknown(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    original_finalize = tools._finalize_kanban_ledger

    def crash_before_finalize(*_args, **_kwargs):
        raise RuntimeError("simulated process death before ledger finalize")

    monkeypatch.setattr(tools, "_finalize_kanban_ledger", crash_before_finalize)
    with pytest.raises(RuntimeError):
        tools._execute_kanban_task_create(
            kb,
            operation_id=_OPERATION_ID,
            title="Deleted in crash window",
            body=None,
            board_input="Hermes G2",
            session_id=None,
            reauthorize=lambda: None,
            cancelled=threading.Event(),
        )
    with kb.connect_closing(board="hermes-g2") as conn:
        task = kb.list_tasks(conn, include_archived=True)[0]
        assert kb.delete_task(conn, task.id) is True
    monkeypatch.setattr(tools, "_finalize_kanban_ledger", original_finalize)

    with pytest.raises(tools._KanbanOperationError) as retry_failure:
        tools._execute_kanban_task_create(
            kb,
            operation_id=_OPERATION_ID,
            title="Deleted in crash window",
            body=None,
            board_input="Hermes G2",
            session_id=None,
            reauthorize=lambda: None,
            cancelled=threading.Event(),
        )
    assert retry_failure.value.error_code == "operation_outcome_unknown"
    assert retry_failure.value.commit_state == "unknown"
    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []


def test_prepared_intent_fails_closed_after_board_generation_replacement(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    _prepare_ledger_state(tools, kb)
    shutil.rmtree(kb.board_dir("hermes-g2"))
    kb.create_board("hermes-g2", name="Hermes G2")

    with pytest.raises(tools._KanbanOperationError) as raised:
        tools._execute_kanban_task_create(
            kb,
            operation_id=_OPERATION_ID,
            title="Recoverable",
            body=None,
            board_input="Hermes G2",
            session_id=None,
            reauthorize=lambda: None,
            cancelled=threading.Event(),
        )
    assert raised.value.error_code == "board_generation_changed"
    assert raised.value.commit_state == "not_committed"
    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("remove_board", [False, True])
async def test_resolve_to_archive_or_delete_toctou_never_recreates_board(
    plugin_package, monkeypatch, tmp_path, remove_board
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    original_mark = tools._mark_kanban_mutating

    def mark_then_change_board(ledger, operation_id):
        entry = original_mark(ledger, operation_id)
        if remove_board:
            shutil.rmtree(kb.board_dir("hermes-g2"))
        else:
            kb.write_board_metadata("hermes-g2", archived=True)
        return entry

    monkeypatch.setattr(tools, "_mark_kanban_mutating", mark_then_change_board)
    result = json.loads(await tools._handle_kanban_task_create({
        "operation_id": _OPERATION_ID,
        "title": "Must never cross the TOCTOU",
        "board": "Hermes G2",
    }))

    assert result["success"] is False
    assert result["commit_state"] == "unknown"
    assert result["error_code"] == "operation_outcome_unknown"
    if remove_board:
        assert not kb.board_dir("hermes-g2").exists()
    else:
        with kb.connect_closing(board="hermes-g2") as conn:
            assert kb.list_tasks(conn, include_archived=True) == []


@pytest.mark.asyncio
async def test_writer_contention_revalidates_revoked_turn_before_mutation(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    mutating = threading.Event()
    original_mark = tools._mark_kanban_mutating

    def mark_and_signal(ledger, operation_id):
        entry = original_mark(ledger, operation_id)
        mutating.set()
        return entry

    monkeypatch.setattr(tools, "_mark_kanban_mutating", mark_and_signal)
    blocker = sqlite3.connect(kb.kanban_db_path("hermes-g2"), isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    task = asyncio.create_task(tools._handle_kanban_task_create({
        "operation_id": _OPERATION_ID,
        "title": "Revoked while waiting",
        "board": "Hermes G2",
    }))
    assert await asyncio.to_thread(mutating.wait, 1.0)
    adapter.active = False
    blocker.execute("ROLLBACK")
    blocker.close()
    result = json.loads(await asyncio.wait_for(task, timeout=2.0))

    assert result["success"] is False
    assert result["commit_state"] == "unknown"
    assert result["error_code"] == "operation_outcome_unknown"
    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []


@pytest.mark.asyncio
async def test_flock_timeout_on_committed_retry_is_typed_unknown_not_authority(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    payload = {
        "operation_id": _OPERATION_ID,
        "title": "Already committed",
        "board": "Hermes G2",
    }
    first = json.loads(await tools._handle_kanban_task_create(payload))
    assert first["success"] is True

    with tools._kanban_operation_lock(threading.Event()):
        retry = json.loads(await tools._handle_kanban_task_create(payload))

    assert retry == {
        "success": False,
        "commit_state": "unknown",
        "operation_id": _OPERATION_ID,
        "error_code": "operation_outcome_unknown",
        "error": (
            "Kanban creation may have started but its exact outcome is unknown"
        ),
    }
    assert "authority" not in retry["error"].lower()
    with kb.connect_closing(board="hermes-g2") as conn:
        assert len(kb.list_tasks(conn, include_archived=True)) == 1


@pytest.mark.asyncio
async def test_detached_attempt_holding_flock_makes_attempt_two_unknown(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    before_finalize = threading.Event()
    release = threading.Event()
    original_finalize = tools._finalize_kanban_ledger

    def pause_then_finalize(*args, **kwargs):
        before_finalize.set()
        assert release.wait(2.0)
        return original_finalize(*args, **kwargs)

    monkeypatch.setattr(tools, "_finalize_kanban_ledger", pause_then_finalize)
    payload = {
        "operation_id": _OPERATION_ID,
        "title": "Response-loss race",
        "board": "Hermes G2",
    }
    attempt_one = asyncio.create_task(
        tools._handle_kanban_task_create(payload)
    )
    assert await asyncio.to_thread(before_finalize.wait, 1.0)
    attempt_two = json.loads(await asyncio.wait_for(
        tools._handle_kanban_task_create(payload), timeout=2.0
    ))
    assert attempt_two["success"] is False
    assert attempt_two["commit_state"] == "unknown"
    assert attempt_two["error_code"] == "operation_outcome_unknown"
    release.set()
    first_result = json.loads(await asyncio.wait_for(attempt_one, timeout=2.0))
    assert first_result["success"] is True
    with kb.connect_closing(board="hermes-g2") as conn:
        tasks = kb.list_tasks(conn, include_archived=True)
    assert [task.id for task in tasks] == [first_result["receipt"]["task_id"]]


@pytest.mark.asyncio
async def test_cancel_during_writer_contention_stops_before_mutation(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    mutating = threading.Event()
    finished = threading.Event()
    original_mark = tools._mark_kanban_mutating
    original_execute = tools._execute_kanban_task_create

    def mark_and_signal(ledger, operation_id):
        entry = original_mark(ledger, operation_id)
        mutating.set()
        return entry

    def execute_and_signal(*args, **kwargs):
        try:
            return original_execute(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(tools, "_mark_kanban_mutating", mark_and_signal)
    monkeypatch.setattr(tools, "_execute_kanban_task_create", execute_and_signal)
    blocker = sqlite3.connect(kb.kanban_db_path("hermes-g2"), isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    task = asyncio.create_task(tools._handle_kanban_task_create({
        "operation_id": _OPERATION_ID,
        "title": "Cancelled while waiting",
        "board": "Hermes G2",
    }))
    assert await asyncio.to_thread(mutating.wait, 1.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    blocker.execute("ROLLBACK")
    blocker.close()
    assert await asyncio.to_thread(finished.wait, 2.0)
    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []


@pytest.mark.asyncio
async def test_cancel_during_create_rolls_back_at_final_in_transaction_check(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    verified = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    original_verify = tools._verify_parked_kanban_task
    original_execute = tools._execute_kanban_task_create

    def verify_then_pause(*args, **kwargs):
        task = original_verify(*args, **kwargs)
        verified.set()
        assert release.wait(2.0)
        return task

    def execute_and_signal(*args, **kwargs):
        try:
            return original_execute(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(tools, "_verify_parked_kanban_task", verify_then_pause)
    monkeypatch.setattr(tools, "_execute_kanban_task_create", execute_and_signal)
    handler = asyncio.create_task(tools._handle_kanban_task_create({
        "operation_id": _OPERATION_ID,
        "title": "Cancel before commit",
        "board": "Hermes G2",
    }))
    assert await asyncio.to_thread(verified.wait, 1.0)
    handler.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handler
    release.set()
    assert await asyncio.to_thread(finished.wait, 2.0)

    with kb.connect_closing(board="hermes-g2") as conn:
        assert kb.list_tasks(conn, include_archived=True) == []
    ledger = Path(os.environ["HERMES_HOME"]) / "state" / "g2-workflows" / (
        "kanban-operations.sqlite3"
    )
    with sqlite3.connect(ledger) as conn:
        state = conn.execute(
            "SELECT state FROM operations WHERE operation_id = ?",
            (_OPERATION_ID,),
        ).fetchone()[0]
    assert state == "MUTATING"


@pytest.mark.asyncio
async def test_cancel_after_board_commit_still_finalizes_durable_tombstone(
    plugin_package, monkeypatch, tmp_path
):
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")
    kb = _prepare_board(monkeypatch, tmp_path)
    adapter = _RevocableAdapter(object())
    _authorize_handler(tools, monkeypatch, adapter)
    before_finalize = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    original_finalize = tools._finalize_kanban_ledger
    original_execute = tools._execute_kanban_task_create

    def pause_then_finalize(*args, **kwargs):
        before_finalize.set()
        assert release.wait(2.0)
        return original_finalize(*args, **kwargs)

    def execute_and_signal(*args, **kwargs):
        try:
            return original_execute(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(tools, "_finalize_kanban_ledger", pause_then_finalize)
    monkeypatch.setattr(tools, "_execute_kanban_task_create", execute_and_signal)
    payload = {
        "operation_id": _OPERATION_ID,
        "title": "Commit then cancel",
        "board": "Hermes G2",
    }
    handler = asyncio.create_task(tools._handle_kanban_task_create(payload))
    assert await asyncio.to_thread(before_finalize.wait, 1.0)
    handler.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handler
    release.set()
    assert await asyncio.to_thread(finished.wait, 2.0)

    ledger = Path(os.environ["HERMES_HOME"]) / "state" / "g2-workflows" / (
        "kanban-operations.sqlite3"
    )
    with sqlite3.connect(ledger) as conn:
        row = conn.execute(
            "SELECT state, task_id FROM operations WHERE operation_id = ?",
            (_OPERATION_ID,),
        ).fetchone()
    assert row is not None and row[0] == "COMMITTED"
    with kb.connect_closing(board="hermes-g2") as conn:
        tasks = kb.list_tasks(conn, include_archived=True)
    assert [task.id for task in tasks] == [row[1]]


def test_public_relay_surface_has_one_static_kanban_create_only(plugin_package):
    relay = importlib.import_module(f"{plugin_package.__name__}.workflow_relay")
    tools = importlib.import_module(f"{plugin_package.__name__}.tools")

    assert relay.WORKFLOW_INTERNAL_SEQUENCE["g2_kanban_task_create"] == (
        "g2.kanban.task.create",
    )
    assert "g2.kanban.task.create" in tools._MCP_WORKFLOW_HANDLERS
    for raw_name in (
        "kanban_create",
        "kanban_list",
        "kanban_update",
        "kanban_delete",
        "kanban_comment",
        "kanban_dispatch",
    ):
        assert raw_name not in tools._MCP_WORKFLOW_HANDLERS
