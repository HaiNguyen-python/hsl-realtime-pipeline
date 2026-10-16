"""Entry point for spark-submit (it needs a file, not a module)."""

import logging

from hsl_pipeline.streaming_job import run

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
