import pytest


@pytest.fixture(autouse=True)
def isolate_experiment_database(tmp_path, monkeypatch):
    """Keep API tests independent while exercising the configurable SQLite path."""
    monkeypatch.setenv(
        "STREAMPULSE_EXPERIMENT_DB_PATH",
        str(tmp_path / "streampulse-experiments.sqlite3"),
    )
