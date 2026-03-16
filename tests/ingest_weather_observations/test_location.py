from ingest_weather_observations.location import extract_geopoint


def test_extract_geopoint_from_latitude_longitude_fields():
    metadata = {"latitude": "30.2672", "longitude": "-97.7431"}
    point = extract_geopoint(metadata)
    assert point is not None
    assert point.latitude == 30.2672
    assert point.longitude == -97.7431


def test_extract_geopoint_from_lat_lon_fields():
    metadata = {"lat": 40.7128, "lon": -74.006}
    point = extract_geopoint(metadata)
    assert point is not None
    assert point.latitude == 40.7128
    assert point.longitude == -74.006


def test_extract_geopoint_from_nested_location():
    metadata = {"location": {"lat": "34.0522", "lon": "-118.2437"}}
    point = extract_geopoint(metadata)
    assert point is not None
    assert point.latitude == 34.0522
    assert point.longitude == -118.2437


def test_extract_geopoint_returns_none_for_invalid_coords():
    metadata = {"latitude": "999", "longitude": "-74.0060"}
    assert extract_geopoint(metadata) is None