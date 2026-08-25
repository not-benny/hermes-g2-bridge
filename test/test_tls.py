from pathlib import Path


def test_bridge_server_supports_configured_tls():
    source = Path(__file__).resolve().parents[1].joinpath("adapter.py").read_text()
    assert "import ssl" in source
    assert 'extra.get("tls_certfile")' in source
    assert 'extra.get("tls_keyfile")' in source
    assert "ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)" in source
    assert "load_cert_chain" in source
    assert "ssl=tls_context" in source
    assert '"wss" if tls_context else "ws"' in source
    assert '"tls_required"' in source
    assert "_is_loopback_address(self.host)" in source
