# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import pytest

import json
from fastavro import schemaless_reader, schemaless_writer
from io import BytesIO

from etptypes import avro_schema

from etptypes.energistics.etp.v12.datatypes.any_array import AnyArray
from etptypes.energistics.etp.v12.datatypes.array_of_boolean import (
    ArrayOfBoolean,
)
from etptypes.energistics.etp.v12.datatypes.array_of_int import ArrayOfInt
from etptypes.energistics.etp.v12.datatypes.array_of_long import ArrayOfLong
from etptypes.energistics.etp.v12.datatypes.array_of_float import ArrayOfFloat
from etptypes.energistics.etp.v12.datatypes.array_of_double import (
    ArrayOfDouble,
)
from etptypes.energistics.etp.v12.datatypes.array_of_string import (
    ArrayOfString,
)


@pytest.mark.asyncio
async def test_get_ArrayOfBoolean() -> None:
    array = AnyArray(item=ArrayOfBoolean(values=[True, False]))

    bio = BytesIO()

    objSchema = json.loads(avro_schema(AnyArray))
    schemaless_writer(
        bio, objSchema, array.dict(by_alias=True), disable_tuple_notation=False
    )

    bio.seek(0)

    object_res = AnyArray.parse_obj(
        schemaless_reader(
            bio,
            json.loads(avro_schema(AnyArray)),
            return_record_name=True,
            return_record_name_override=True,
        )
    )

    assert isinstance(object_res, AnyArray)
    assert isinstance(object_res.item, ArrayOfBoolean)


@pytest.mark.asyncio
async def test_get_ArrayOfFloat() -> None:
    array = AnyArray(item=ArrayOfFloat(values=[1.0, 2.3, 4.0, 5.5]))

    bio = BytesIO()

    objSchema = json.loads(avro_schema(AnyArray))
    schemaless_writer(
        bio, objSchema, array.dict(by_alias=True), disable_tuple_notation=False
    )

    bio.seek(0)

    object_res = AnyArray.parse_obj(
        schemaless_reader(
            bio,
            json.loads(avro_schema(AnyArray)),
            return_record_name=True,
            return_record_name_override=True,
        )
    )

    assert isinstance(object_res, AnyArray)
    assert isinstance(object_res.item, ArrayOfFloat)


@pytest.mark.asyncio
async def test_get_ArrayOfDouble() -> None:
    array = AnyArray(item=ArrayOfDouble(values=[1.0, 2.3, 4.0, 5.5]))

    bio = BytesIO()

    objSchema = json.loads(avro_schema(AnyArray))
    schemaless_writer(
        bio, objSchema, array.dict(by_alias=True), disable_tuple_notation=False
    )

    bio.seek(0)

    object_res = AnyArray.parse_obj(
        schemaless_reader(
            bio,
            json.loads(avro_schema(AnyArray)),
            return_record_name=True,
            return_record_name_override=True,
        )
    )

    assert isinstance(object_res, AnyArray)
    assert isinstance(object_res.item, ArrayOfDouble)


@pytest.mark.asyncio
async def test_get_ArrayOfInt() -> None:
    array = AnyArray(item=ArrayOfInt(values=[1, 2, 4, 5]))

    bio = BytesIO()

    objSchema = json.loads(avro_schema(AnyArray))
    schemaless_writer(
        bio, objSchema, array.dict(by_alias=True), disable_tuple_notation=False
    )

    bio.seek(0)

    object_res = AnyArray.parse_obj(
        schemaless_reader(
            bio,
            json.loads(avro_schema(AnyArray)),
            return_record_name=True,
            return_record_name_override=True,
        )
    )

    assert isinstance(object_res, AnyArray)
    assert isinstance(object_res.item, ArrayOfInt)


@pytest.mark.asyncio
async def test_get_ArrayOfLong() -> None:
    array = AnyArray(item=ArrayOfLong(values=[1, 2, 4, 5]))

    bio = BytesIO()

    objSchema = json.loads(avro_schema(AnyArray))
    schemaless_writer(
        bio, objSchema, array.dict(by_alias=True), disable_tuple_notation=False
    )

    bio.seek(0)

    object_res = AnyArray.parse_obj(
        schemaless_reader(
            bio,
            json.loads(avro_schema(AnyArray)),
            return_record_name=True,
            return_record_name_override=True,
        )
    )

    assert isinstance(object_res, AnyArray)
    assert isinstance(object_res.item, ArrayOfLong)


@pytest.mark.asyncio
async def test_get_ArrayOfString() -> None:
    array = AnyArray(item=ArrayOfString(values=["1", "2", "4", "5"]))

    bio = BytesIO()

    objSchema = json.loads(avro_schema(AnyArray))
    schemaless_writer(
        bio, objSchema, array.dict(by_alias=True), disable_tuple_notation=False
    )

    bio.seek(0)

    object_res = AnyArray.parse_obj(
        schemaless_reader(
            bio,
            json.loads(avro_schema(AnyArray)),
            return_record_name=True,
            return_record_name_override=True,
        )
    )

    assert isinstance(object_res, AnyArray)
    assert isinstance(object_res.item, ArrayOfString)


@pytest.mark.asyncio
async def test_get_bytes() -> None:
    array = AnyArray(item=b"coucou")

    bio = BytesIO()

    objSchema = json.loads(avro_schema(AnyArray))
    schemaless_writer(
        bio, objSchema, array.dict(by_alias=True), disable_tuple_notation=False
    )

    bio.seek(0)

    object_res = AnyArray.parse_obj(
        schemaless_reader(
            bio,
            json.loads(avro_schema(AnyArray)),
            return_record_name=True,
            return_record_name_override=True,
        )
    )

    assert isinstance(object_res, AnyArray)
    assert isinstance(object_res.item, bytes)
