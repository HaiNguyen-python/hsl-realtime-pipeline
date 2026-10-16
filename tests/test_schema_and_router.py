import json

import pytest
from tests_helpers import TOPIC, payload

from hsl_pipeline.router import MessageRouter
from hsl_pipeline.schema import InvalidMessage, mode_from_topic, parse_hfp


def test_parses_valid_message():
    e = parse_hfp(TOPIC, payload())
    assert e.mode == "tram"
    assert e.event_id == "40:412:1792047605"
    assert e.delay_s == 45  # dl=-45 means 45 s behind schedule
    assert e.event_time.tzinfo is not None


@pytest.mark.parametrize(
    "body, reason",
    [
        ("{not json", "not JSON"),
        (json.dumps({"other": 1}), "no 'VP'"),
        (payload(dl=None), "missing fields: dl"),
        (payload(lat=0.0), "outside HSL area"),
        (payload(spd=500), "validation failed"),
    ],
)
def test_rejects_bad_messages(body, reason):
    with pytest.raises(InvalidMessage, match=reason):
        parse_hfp(TOPIC, body)


def test_mode_from_topic_rejects_unknown_format():
    with pytest.raises(InvalidMessage):
        mode_from_topic("/something/else")


def test_router_sends_valid_to_main_and_invalid_to_dlq():
    sent = []
    router = MessageRouter(lambda t, k, v: sent.append((t, k, v)), "main", "dlq")
    assert router.handle(TOPIC, payload().encode())
    assert not router.handle(TOPIC, b"{broken")
    assert [s[0] for s in sent] == ["main", "dlq"]
    assert sent[0][1] == "40:412"  # keyed by vehicle for per-vehicle ordering
    assert json.loads(sent[1][2])["reason"].startswith("payload is not JSON")
    assert router.stats == {"valid": 1, "dead_letter": 1}
