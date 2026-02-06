"""Tests for data models."""

import json
from datetime import datetime, timezone

from beads.models import (
    Comment, Dependency, DepType, Issue, IssueType, Status,
    format_timestamp, parse_timestamp,
)


def test_issue_defaults():
    issue = Issue()
    issue.set_defaults()
    assert issue.status == Status.OPEN
    assert issue.issue_type == IssueType.TASK


def test_issue_validate_empty_title():
    issue = Issue(title="")
    assert issue.validate() is not None
    assert "title is required" in issue.validate()


def test_issue_validate_priority():
    issue = Issue(title="test", priority=5)
    assert "priority must be between 0 and 4" in issue.validate()


def test_issue_validate_closed_without_closed_at():
    issue = Issue(title="test", status=Status.CLOSED)
    assert "closed issues must have closed_at" in issue.validate()


def test_issue_validate_valid():
    issue = Issue(title="test", priority=2, status=Status.OPEN)
    assert issue.validate() is None


def test_issue_to_dict_omitempty():
    """Test that empty fields are omitted (matching Go's omitempty)."""
    issue = Issue(
        id="test-abc123",
        title="Test Issue",
        status=Status.OPEN,
        priority=2,
        issue_type=IssueType.TASK,
        created_at=datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
    )
    d = issue.to_dict()
    assert d["id"] == "test-abc123"
    assert d["title"] == "Test Issue"
    assert d["priority"] == 2
    # Empty description should not be in dict
    assert "description" not in d
    assert "design" not in d
    assert "labels" not in d


def test_issue_to_dict_with_labels():
    issue = Issue(id="test-1", title="t", labels=["urgent", "backend"])
    d = issue.to_dict()
    assert d["labels"] == ["urgent", "backend"]


def test_issue_roundtrip():
    """Test serialization/deserialization roundtrip."""
    now = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    issue = Issue(
        id="test-abc123",
        title="Test Issue",
        description="A test",
        status=Status.IN_PROGRESS,
        priority=1,
        issue_type=IssueType.BUG,
        assignee="alice",
        created_at=now,
        updated_at=now,
        labels=["urgent"],
    )
    d = issue.to_dict()
    json_str = json.dumps(d)
    restored = Issue.from_dict(json.loads(json_str))
    assert restored.id == issue.id
    assert restored.title == issue.title
    assert restored.description == issue.description
    assert restored.status == issue.status
    assert restored.priority == issue.priority
    assert restored.issue_type == issue.issue_type
    assert restored.assignee == issue.assignee
    assert restored.labels == issue.labels


def test_content_hash_deterministic():
    """Content hash should be deterministic for same content."""
    issue1 = Issue(title="Test", description="Desc", status=Status.OPEN, priority=2)
    issue2 = Issue(title="Test", description="Desc", status=Status.OPEN, priority=2)
    assert issue1.compute_content_hash() == issue2.compute_content_hash()


def test_content_hash_changes_with_content():
    issue1 = Issue(title="Test A", priority=2)
    issue2 = Issue(title="Test B", priority=2)
    assert issue1.compute_content_hash() != issue2.compute_content_hash()


def test_priority_zero_serialized():
    """Priority 0 (P0/critical) must be serialized even though it's zero."""
    issue = Issue(id="t", title="t", priority=0)
    d = issue.to_dict()
    assert "priority" in d
    assert d["priority"] == 0


def test_timestamp_roundtrip():
    ts = "2026-01-15T10:00:00Z"
    dt = parse_timestamp(ts)
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 1
    assert dt.hour == 10
    formatted = format_timestamp(dt)
    assert formatted == ts


def test_dependency_to_dict():
    dep = Dependency(
        issue_id="test-1",
        depends_on_id="test-2",
        type=DepType.BLOCKS,
        created_by="alice",
    )
    d = dep.to_dict()
    assert d["issue_id"] == "test-1"
    assert d["depends_on_id"] == "test-2"
    assert d["type"] == "blocks"


def test_dep_type_affects_ready():
    assert DepType.affects_ready_work("blocks")
    assert DepType.affects_ready_work("parent-child")
    assert not DepType.affects_ready_work("related")
    assert not DepType.affects_ready_work("relates-to")


def test_issue_type_normalize():
    assert IssueType.normalize("enhancement") == IssueType.FEATURE
    assert IssueType.normalize("feat") == IssueType.FEATURE
    assert IssueType.normalize("bug") == IssueType.BUG
