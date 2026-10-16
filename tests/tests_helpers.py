"""Shared HFP test fixtures."""

import json

TOPIC = "/hfp/v2/journey/ongoing/vp/tram/0040/00412/1009/1/Kirurgi/12:31/1130106/4/60;24/19/73/42"


def payload(**overrides):
    vp = {
        "desi": "9",
        "dir": "1",
        "oper": 40,
        "veh": 412,
        "tst": "2026-10-15T07:00:05.000Z",
        "tsi": 1792047605,
        "spd": 5.2,
        "lat": 60.17,
        "long": 24.94,
        "dl": -45,
        "route": "1009",
    }
    vp.update(overrides)
    return json.dumps({"VP": vp})
