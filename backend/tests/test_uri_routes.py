import pytest
from fastapi import HTTPException
from utils.media_uri import MediaURI
from routers.shows import parse_show_id_or_uri
from routers.media import parse_person_id_param


def test_parse_show_id_or_uri():
    assert parse_show_id_or_uri(5920) == ("tmdb", 5920, "tmdb:s:5920")
    assert parse_show_id_or_uri("5920") == ("tmdb", 5920, "tmdb:s:5920")
    assert parse_show_id_or_uri("tmdb:s:5920") == ("tmdb", 5920, "tmdb:s:5920")
    assert parse_show_id_or_uri("tvdb:s:82459") == ("tvdb", 82459, "tvdb:s:82459")
    assert parse_show_id_or_uri("tvdb/82459") == ("tvdb", 82459, "tvdb:s:82459")

    with pytest.raises(HTTPException):
        parse_show_id_or_uri("invalid_id")


def test_parse_person_id_param():
    assert parse_person_id_param(2046128) == ("tmdb", 2046128, "tmdb:p:2046128")
    assert parse_person_id_param("2046128") == ("tmdb", 2046128, "tmdb:p:2046128")
    assert parse_person_id_param("tmdb:p:2046128") == ("tmdb", 2046128, "tmdb:p:2046128")
    assert parse_person_id_param("tvdb:p:12345") == ("tvdb", 12345, "tvdb:p:12345")

    with pytest.raises(HTTPException):
        parse_person_id_param("invalid_person")


def test_media_uri_person():
    uri = MediaURI.parse("tmdb:p:2046128")
    assert uri.provider == "tmdb"
    assert uri.type_prefix == "p"
    assert uri.id == "2046128"
    assert uri.media_type == "person"
    assert str(uri) == "tmdb:p:2046128"
