from datetime import datetime, timezone

import pytest

from app.processing.metrics import Metrics, calculate_metrics
from app.processing.window_manager import SlidingWindow, TumblingWindow
from app.schemas import EventType, LibraryEvent


def make_event(value=1, event_id="event-1", event_type=EventType.BOOK_ISSUED):
    return LibraryEvent(
        event_id=event_id,
        event_type=event_type,
        student_id="student-1",
        book_id="book-1",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        value=value,
    )


def test_event_schema_accepts_supported_types_and_iso_timestamp():
    event = LibraryEvent.from_mapping({
        "event_id": "e1",
        "event_type": "BOOK_RESERVED",
        "student_id": "s1",
        "book_id": "b1",
        "timestamp": "2026-01-01T00:00:00Z",
        "value": 1,
    })
    assert event.event_type is EventType.BOOK_RESERVED
    assert event.timestamp == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert event.value == 1.0


@pytest.mark.parametrize("event_type", list(EventType))
def test_event_schema_accepts_each_supported_event_type(event_type):
    assert make_event(event_type=event_type).event_type is event_type


def test_event_schema_rejects_unsupported_event_type():
    with pytest.raises(ValueError, match="event_type"):
        make_event(event_type="BOOK_LOST")


@pytest.mark.parametrize("bad_value", [True, "1", float("nan"), float("inf")])
def test_event_schema_rejects_invalid_numeric_values(bad_value):
    with pytest.raises(ValueError):
        make_event(value=bad_value)


def test_event_schema_rejects_missing_or_unknown_fields():
    with pytest.raises(ValueError, match="missing"):
        LibraryEvent.from_mapping({"event_id": "e1"})
    with pytest.raises(ValueError, match="unexpected"):
        LibraryEvent.from_mapping({
            "event_id": "e1", "event_type": "BOOK_ISSUED", "student_id": "s1",
            "book_id": "b1", "timestamp": datetime.now(), "value": 1, "extra": 2,
        })


def test_metrics_for_empty_input():
    assert calculate_metrics([]) == Metrics(0, 0.0, None, None, None)


def test_metrics_calculate_all_statistics():
    result = calculate_metrics([make_event(2), make_event(4), make_event(-1)])
    assert result == Metrics(count=3, total=5.0, average=5 / 3, minimum=-1.0, maximum=4.0)


def test_tumbling_window_waits_for_complete_window():
    window = TumblingWindow(3)
    assert window.add(make_event(2)) is None
    assert window.pending_count == 1
    assert window.add(make_event(4)) is None
    assert window.pending_count == 2
    assert window.add(make_event(6)) == Metrics(3, 12.0, 4.0, 2.0, 6.0)
    assert window.pending_count == 0


def test_tumbling_windows_are_non_overlapping():
    window = TumblingWindow(2)
    assert window.add(make_event(1)) is None
    assert window.add(make_event(3)) == Metrics(2, 4.0, 2.0, 1.0, 3.0)
    assert window.add(make_event(10)) is None
    assert window.pending_count == 1


def test_sliding_window_reports_partial_results_then_rolls_over():
    window = SlidingWindow(3)
    assert window.add(make_event(2, "e1")) == Metrics(1, 2.0, 2.0, 2.0, 2.0)
    assert window.add(make_event(4, "e2")) == Metrics(2, 6.0, 3.0, 2.0, 4.0)
    assert window.add(make_event(6, "e3")) == Metrics(3, 12.0, 4.0, 2.0, 6.0)
    assert window.add(make_event(8, "e4")) == Metrics(3, 18.0, 6.0, 4.0, 8.0)
    assert window.event_count == 3


@pytest.mark.parametrize("window_class", [TumblingWindow, SlidingWindow])
@pytest.mark.parametrize("size", [0, -1, 1.5, True])
def test_windows_reject_invalid_sizes(window_class, size):
    with pytest.raises(ValueError):
        window_class(size)
