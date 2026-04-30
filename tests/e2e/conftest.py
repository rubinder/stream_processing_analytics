import os
import time

import pytest
import requests

PINOT = os.environ.get("PINOT_URL", "http://localhost:8099/query/sql")


def pinot_query(sql: str):
    r = requests.post(PINOT, json={"sql": sql}, timeout=10)
    r.raise_for_status()
    body = r.json()
    if body.get("exceptions"):
        raise RuntimeError(body["exceptions"])
    cols = body["resultTable"]["dataSchema"]["columnNames"]
    return [dict(zip(cols, row)) for row in body["resultTable"]["rows"]]


def wait_for(predicate, timeout=15, interval=0.5):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


@pytest.fixture
def query():
    return pinot_query


@pytest.fixture
def waitfor():
    return wait_for
