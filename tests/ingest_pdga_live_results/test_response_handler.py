from ingest_pdga_live_results.response_handler import (
    canonical_json,
    classify_response,
    compute_payload_sha256,
    is_empty_payload,
)


def test_canonical_json_stable_key_order():
    payload_a = {"b": 2, "a": 1}
    payload_b = {"a": 1, "b": 2}
    assert canonical_json(payload_a) == canonical_json(payload_b)


def test_compute_payload_sha256_stable_across_key_order():
    payload_a = {"b": 2, "a": 1}
    payload_b = {"a": 1, "b": 2}
    assert compute_payload_sha256(payload_a) == compute_payload_sha256(payload_b)


def test_is_empty_payload_true_for_none_or_empty_structures():
    assert is_empty_payload(None) is True
    assert is_empty_payload({}) is True
    assert is_empty_payload([]) is True
    assert is_empty_payload({"results": []}) is True
    assert is_empty_payload({"players": []}) is True


def test_is_empty_payload_false_for_non_empty_payload():
    assert is_empty_payload({"results": [{"score": 55}]}) is False
    assert is_empty_payload([{"player": "A"}]) is False


def test_classify_response_not_found():
    assert classify_response(status_code=404, payload=None) == "not_found"


def test_classify_response_empty_200():
    assert classify_response(status_code=200, payload=[]) == "empty"


def test_classify_response_success_200():
    assert classify_response(status_code=200, payload={"results": [{"a": 1}]}) == "success"


def test_classify_response_failed_non_200():
    assert classify_response(status_code=500, payload={"results": []}) == "failed"


def test_classify_response_failed_when_error_present():
    assert classify_response(status_code=200, payload={"results": [{"a": 1}]}, error=RuntimeError("boom")) == "failed"