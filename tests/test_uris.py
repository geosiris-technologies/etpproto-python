import pytest

from etpproto.uri import *


def test_uri_dataspace_0() -> None:
    uri = parse_uri("eml:///dataspace('/folder-name/project-name')")
    assert isinstance(uri, DataspaceUri)
    assert uri.dataspace == "/folder-name/project-name"


def test_uri_dataspace_1() -> None:
    uri = parse_uri("eml:///dataspace('alwyn')")
    assert isinstance(uri, DataspaceUri)
    assert uri.dataspace == "alwyn"


def test_uri_dataspace_2() -> None:
    uri = parse_uri("eml:///")
    assert isinstance(uri, DataspaceUri)
    assert uri.dataspace is None


def test_uri_data_object_0() -> None:
    uri = parse_uri(
        "eml:///witsml20.ChannelSet(2c0f6ef2-cc54-4104-8523-0f0fbaba3661)"
    )
    assert isinstance(uri, DataObjectURI)
    assert uri.dataspace is None
    assert uri.domain == "witsml"
    assert uri.domain_version == "20"
    assert uri.object_type == "ChannelSet"
    assert uri.uuid == "2c0f6ef2-cc54-4104-8523-0f0fbaba3661"
    assert uri.version is None


def test_uri_data_object_1() -> None:
    uri = parse_uri(
        "eml:///dataspace('alwyn')/witsml20.ChannelSet(2c0f6ef2-cc54-4104-8523-0f0fbaba3661)"
    )
    assert isinstance(uri, DataObjectURI)
    assert uri.dataspace == "alwyn"
    assert uri.domain == "witsml"
    assert uri.domain_version == "20"
    assert uri.object_type == "ChannelSet"
    assert uri.uuid == "2c0f6ef2-cc54-4104-8523-0f0fbaba3661"
    assert uri.version is None


def test_uri_data_object_2() -> None:
    uri = parse_uri(
        "eml:///dataspace('rdms-db')/resqml20.obj_HorizonInterpretation(uuid=421a7a05-033a-450d-bcef-051352023578,version='2.0')"
    )
    assert isinstance(uri, DataObjectURI)
    assert uri.dataspace == "rdms-db"
    assert uri.domain == "resqml"
    assert uri.domain_version == "20"
    assert uri.object_type == "obj_HorizonInterpretation"
    assert uri.uuid == "421a7a05-033a-450d-bcef-051352023578"
    assert uri.version == "2.0"


def test_raw_uri() -> None:
    uri = parse_uri("a_random_str")
    assert isinstance(uri, Uri)
    assert uri.raw_uri == "a_random_str"
