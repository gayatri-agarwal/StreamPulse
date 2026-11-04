import pytest

from app.processing.window_config import WindowConfiguration


def test_window_configuration_defaults():
    assert WindowConfiguration() == WindowConfiguration(
        tumbling_size=10,
        sliding_size=5,
        sliding_interval=1,
    )


@pytest.mark.parametrize("size", WindowConfiguration.ALLOWED_WINDOW_SIZES)
def test_all_supported_sizes_are_valid_for_each_window(size):
    configuration = WindowConfiguration(
        tumbling_size=size,
        sliding_size=size,
        sliding_interval=1,
    )

    assert configuration.tumbling_size == size
    assert configuration.sliding_size == size


@pytest.mark.parametrize(
    ("tumbling_size", "sliding_size", "sliding_interval"),
    [(5, 5, 4), (8, 8, 7), (10, 10, 9), (5, 10, 9), (10, 5, 2)],
)
def test_valid_sliding_intervals_are_independent_of_tumbling_size(
    tumbling_size, sliding_size, sliding_interval
):
    configuration = WindowConfiguration(
        tumbling_size=tumbling_size,
        sliding_size=sliding_size,
        sliding_interval=sliding_interval,
    )

    assert configuration.sliding_interval == sliding_interval


@pytest.mark.parametrize("field_name", ["tumbling_size", "sliding_size"])
@pytest.mark.parametrize("size", [0, -1, 4, 6, 7, 9, 11])
def test_unsupported_window_sizes_are_rejected(field_name, size):
    with pytest.raises(ValueError, match="window sizes must be one of 5, 8, or 10"):
        WindowConfiguration(**{field_name: size})


@pytest.mark.parametrize("field_name", ["tumbling_size", "sliding_size"])
@pytest.mark.parametrize("value", [True, False, 5.0, "5"])
def test_non_integer_window_sizes_are_rejected(field_name, value):
    with pytest.raises(ValueError, match=f"{field_name} must be an integer"):
        WindowConfiguration(**{field_name: value})


@pytest.mark.parametrize("interval", [0, -1])
def test_non_positive_sliding_intervals_are_rejected(interval):
    with pytest.raises(ValueError, match="sliding_interval must be a positive integer"):
        WindowConfiguration(sliding_interval=interval)


@pytest.mark.parametrize("interval", [True, False, 1.0, "1"])
def test_non_integer_sliding_intervals_are_rejected(interval):
    with pytest.raises(ValueError, match="sliding_interval must be a positive integer"):
        WindowConfiguration(sliding_interval=interval)


@pytest.mark.parametrize("sliding_size", WindowConfiguration.ALLOWED_WINDOW_SIZES)
def test_sliding_interval_equal_to_window_size_is_rejected(sliding_size):
    with pytest.raises(ValueError, match="sliding_interval must be smaller than sliding_size"):
        WindowConfiguration(sliding_size=sliding_size, sliding_interval=sliding_size)


def test_sliding_interval_greater_than_window_size_is_rejected():
    with pytest.raises(ValueError, match="sliding_interval must be smaller than sliding_size"):
        WindowConfiguration(sliding_size=5, sliding_interval=6)
