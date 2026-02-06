"""Tests for ID generation."""

from datetime import datetime, timezone

from beads.id_gen import (
    encode_base36, generate_base36_hash_id, generate_child_id,
    generate_hash_id, make_issue_id, parse_hierarchical_id,
    check_hierarchy_depth,
)


def test_generate_hash_id_deterministic():
    """Same inputs should produce same hash."""
    ts = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    h1 = generate_hash_id("test", "Title", "Desc", ts, "ws1")
    h2 = generate_hash_id("test", "Title", "Desc", ts, "ws1")
    assert h1 == h2
    assert len(h1) == 64  # Full SHA256 hex


def test_generate_hash_id_differs():
    ts = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    h1 = generate_hash_id("test", "Title A", "Desc", ts, "ws1")
    h2 = generate_hash_id("test", "Title B", "Desc", ts, "ws1")
    assert h1 != h2


def test_make_issue_id():
    assert make_issue_id("bd", "abcdef1234567890", 6) == "bd-abcdef"
    assert make_issue_id("bd", "abcdef1234567890", 8) == "bd-abcdef12"


def test_generate_child_id():
    assert generate_child_id("bd-abc123", 1) == "bd-abc123.1"
    assert generate_child_id("bd-abc123.1", 2) == "bd-abc123.1.2"


def test_parse_hierarchical_id_root():
    root, parent, depth = parse_hierarchical_id("bd-abc123")
    assert root == "bd-abc123"
    assert parent == ""
    assert depth == 0


def test_parse_hierarchical_id_child():
    root, parent, depth = parse_hierarchical_id("bd-abc123.1")
    assert root == "bd-abc123"
    assert parent == "bd-abc123"
    assert depth == 1


def test_parse_hierarchical_id_grandchild():
    root, parent, depth = parse_hierarchical_id("bd-abc123.1.2")
    assert root == "bd-abc123"
    assert parent == "bd-abc123.1"
    assert depth == 2


def test_check_hierarchy_depth():
    assert check_hierarchy_depth("bd-abc") is None  # depth=0, can add child
    assert check_hierarchy_depth("bd-abc.1") is None  # depth=1
    assert check_hierarchy_depth("bd-abc.1.2") is None  # depth=2
    assert check_hierarchy_depth("bd-abc.1.2.3") is not None  # depth=3, max reached


def test_encode_base36():
    result = encode_base36(b"\x01\x00", 3)
    assert len(result) == 3
    assert all(c in "0123456789abcdefghijklmnopqrstuvwxyz" for c in result)


def test_base36_id_deterministic():
    ts = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    id1 = generate_base36_hash_id("bd", "Title", "Desc", "alice", ts)
    id2 = generate_base36_hash_id("bd", "Title", "Desc", "alice", ts)
    assert id1 == id2
    assert id1.startswith("bd-")
    assert len(id1.split("-")[1]) == 6
