from silver_pdga_live_results.bronze_io import compute_event_source_fingerprint
from silver_pdga_live_results.models import BronzeRoundSource


def test_event_source_fingerprint_is_order_independent():
    a = BronzeRoundSource(
        event_id=90008,
        division="MA3",
        round_number=1,
        source_json_key="a.json",
        source_meta_key="a.meta.json",
        source_content_sha256="sha-a",
        source_fetched_at_utc="2025-05-17T22:35:04Z",
        payload={"data": {"scores": []}},
    )
    b = BronzeRoundSource(
        event_id=90008,
        division="MA3",
        round_number=2,
        source_json_key="b.json",
        source_meta_key="b.meta.json",
        source_content_sha256="sha-b",
        source_fetched_at_utc="2025-05-18T22:35:04Z",
        payload={"data": {"scores": []}},
    )

    fp1 = compute_event_source_fingerprint([a, b])
    fp2 = compute_event_source_fingerprint([b, a])

    assert fp1 == fp2


def test_event_source_fingerprint_changes_when_source_hash_changes():
    a1 = BronzeRoundSource(
        event_id=90008,
        division="MA3",
        round_number=1,
        source_json_key="a.json",
        source_meta_key="a.meta.json",
        source_content_sha256="sha-a1",
        source_fetched_at_utc="2025-05-17T22:35:04Z",
        payload={"data": {"scores": []}},
    )
    a2 = BronzeRoundSource(
        event_id=90008,
        division="MA3",
        round_number=1,
        source_json_key="a.json",
        source_meta_key="a.meta.json",
        source_content_sha256="sha-a2",
        source_fetched_at_utc="2025-05-17T22:35:04Z",
        payload={"data": {"scores": []}},
    )

    assert compute_event_source_fingerprint([a1]) != compute_event_source_fingerprint([a2])