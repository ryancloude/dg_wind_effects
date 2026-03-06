from ingest_pdga_live_results.response_handler import (
    classify_response,
    compute_payload_sha256,
    is_empty_payload,
)


def test_classify_not_found_404():
    assert classify_response(status_code=404, payload=None) == "not_found"


def test_classify_empty_200():
    assert classify_response(status_code=200, payload=[]) == "empty"
    assert is_empty_payload({"results": []}) is True


def test_classify_success_200():
    assert classify_response(status_code=200, payload={"results": [{"a": 1}]}) == "success"


def test_compute_payload_sha256_stable_across_key_order():
    payload_a = {"b": 2, "a": 1}
    payload_b = {"a": 1, "b": 2}
    assert compute_payload_sha256(payload_a) == compute_payload_sha256(payload_b)