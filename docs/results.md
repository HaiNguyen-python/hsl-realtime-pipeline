Synthetic HFP sample (30 minutes, 30 trams), run through the same validation and Spark transformations as production:

| Step | Count |
| --- | --- |
| Raw MQTT messages | 10,971 |
| Sent to dead-letter topic (bad JSON, GPS errors, missing fields) | 67 |
| Valid events | 10,904 |
| Duplicate deliveries removed | 104 |
| Events counted in delay windows | 10,800 |
