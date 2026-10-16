import pytest

pytest.importorskip("matplotlib")
pytest.importorskip("pyspark")


def test_report_counts_and_chart(tmp_path, monkeypatch):
    monkeypatch.setenv("MPLBACKEND", "Agg")
    from hsl_pipeline import report

    windows, stats = report.run_sample(minutes=6)
    # Every valid event is either a duplicate or counted exactly once in a window.
    assert stats["counted_events"] == stats["unique_events"]
    assert stats["valid"] + stats["dead_letter"] == stats["raw_messages"]
    assert set(windows["route"]) >= {"1004"}

    report.plot(windows, tmp_path / "img" / "delay.png")
    assert (tmp_path / "img" / "delay.png").stat().st_size > 10_000
    assert "Duplicate deliveries removed" in report.results_markdown(stats)
