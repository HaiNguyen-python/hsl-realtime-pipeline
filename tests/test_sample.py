from hsl_pipeline.router import MessageRouter
from hsl_pipeline.sample import generate


def test_sample_contains_messy_cases_and_mostly_valid_events():
    records = generate(minutes=5, vehicles_per_route=1)
    router = MessageRouter(lambda *a: None, "main", "dlq")
    for r in records:
        router.handle(r["topic"], r["payload"])
    assert router.stats["dead_letter"] > 0
    assert router.stats["valid"] > 0.97 * len(records)
