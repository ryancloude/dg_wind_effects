from ingest_weather_observations.geocode import (
    build_geocode_query,
    build_geocode_search_candidates,
    pick_best_geocode_result,
)


def test_build_geocode_query_is_deterministic():
    meta_a = {"city": " Austin ", "state": " Virginia ", "country": "United States"}
    meta_b = {"city": "Austin", "state": "Virginia", "country": "United States"}
    q1 = build_geocode_query(meta_a)
    q2 = build_geocode_query(meta_b)

    assert q1 is not None and q2 is not None
    assert q1.fingerprint == q2.fingerprint
    assert q1.country_code == "US"


def test_build_geocode_search_candidates_progressively_broadens():
    q = build_geocode_query({"city": "Roanoke", "state": "Virginia", "country": "United States"})
    assert q is not None
    candidates = build_geocode_search_candidates(q)
    assert ("Roanoke, Virginia", "US") in candidates
    assert ("Roanoke", "US") in candidates
    assert ("Roanoke", None) in candidates


def test_pick_best_geocode_result_prefers_exact_city_state_country():
    query = build_geocode_query({"city": "Austin", "state": "Texas", "country": "United States"})
    assert query is not None

    payload = {
        "results": [
            {
                "name": "Austin",
                "admin1": "Texas",
                "country": "United States",
                "country_code": "US",
                "latitude": 30.2672,
                "longitude": -97.7431,
                "population": 1000000,
            },
            {
                "name": "Austin",
                "admin1": "Minnesota",
                "country": "United States",
                "country_code": "US",
                "latitude": 43.6666,
                "longitude": -92.9746,
                "population": 25000,
            },
        ]
    }

    best = pick_best_geocode_result(payload, query=query)
    assert best is not None
    assert round(best.point.latitude, 4) == 30.2672