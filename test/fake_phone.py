#!/usr/bin/env python3
"""Simulated Hermes G2 phone for bridge smoke tests without hardware."""

from __future__ import annotations

import argparse
import asyncio
import json

import websockets


def frame(**values):
    return json.dumps({"v": 1, **values})


async def run(uri: str, token: str, utterance: str | None) -> None:
    async with websockets.connect(uri, max_size=1_048_576) as ws:
        await ws.send(frame(chan="ctl", type="hello", token=token, deviceName="fake-hermes-g2"))
        turn_id = "fake-turn-1"
        sent_turn = False
        while True:
            message = json.loads(await ws.recv())
            channel = message.get("chan")
            if channel == "ctl" and message.get("type") == "hello-ack":
                print(f"connected: {message.get('serverName')} session={message.get('sessionKey')}")
                if utterance and not sent_turn:
                    await ws.send(
                        frame(
                            chan="chat",
                            type="utterance",
                            turnId=turn_id,
                            text=utterance,
                            ctx={"foregroundApp": "dashboard", "screenOn": True},
                        )
                    )
                    sent_turn = True
                continue
            if channel == "mcp":
                request = message.get("msg") or {}
                method = request.get("method")
                if method == "initialize":
                    result = {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {"tools": {"listChanged": True}},
                        "serverInfo": {"name": "fake-hermes-g2", "version": "1"},
                    }
                elif method == "tools/list":
                    result = {
                        "tools": [
                            {
                                "name": "glasses.show_alert",
                                "description": "Show a fake alert",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {"text": {"type": "string"}},
                                    "required": ["text"],
                                },
                            }
                        ]
                    }
                elif method == "tools/call":
                    params = request.get("params") or {}
                    result = {
                        "content": [
                            {
                                "type": "text",
                                "text": f"fake call {params.get('name')}: {params.get('arguments')}",
                            }
                        ]
                    }
                elif method == "ping":
                    result = {}
                else:
                    await ws.send(
                        frame(
                            chan="mcp",
                            msg={
                                "jsonrpc": "2.0",
                                "id": request.get("id"),
                                "error": {"code": -32601, "message": f"unsupported: {method}"},
                            },
                        )
                    )
                    continue
                await ws.send(
                    frame(
                        chan="mcp",
                        msg={"jsonrpc": "2.0", "id": request.get("id"), "result": result},
                    )
                )
                continue
            if channel == "chat":
                print(json.dumps(message, ensure_ascii=False))
                if message.get("turnId") == turn_id and message.get("type") in {
                    "turn-done",
                    "turn-error",
                }:
                    return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("uri", help="Bridge URL, e.g. ws://127.0.0.1:8790")
    parser.add_argument("token")
    parser.add_argument("utterance", nargs="?")
    args = parser.parse_args()
    asyncio.run(run(args.uri, args.token, args.utterance))


if __name__ == "__main__":
    main()
