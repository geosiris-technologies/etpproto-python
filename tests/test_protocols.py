import asyncio
import pytest

from etptypes.energistics.etp.v12.datatypes.data_array_types.data_array_identifier import (
    DataArrayIdentifier,
)
from etptypes.energistics.etp.v12.datatypes.data_array_types.get_data_subarrays_type import (
    GetDataSubarraysType,
)
from etptypes.energistics.etp.v12.datatypes.data_array_types.put_data_arrays_type import (
    PutDataArraysType,
)
from etptypes.energistics.etp.v12.datatypes.data_array_types.put_data_subarrays_type import (
    PutDataSubarraysType,
)
from etptypes.energistics.etp.v12.datatypes.data_array_types.put_uninitialized_data_array_type import (
    PutUninitializedDataArrayType,
)
from etptypes.energistics.etp.v12.datatypes.data_array_types.data_array_metadata import (
    DataArrayMetadata,
)
from etptypes.energistics.etp.v12.datatypes.data_array_types.data_array import (
    DataArray,
)

try:
    from .server_protocol_example import *
except Exception:
    from server_protocol_example import *


#     ____        __
#    / __ \____ _/ /_____ __________  ____ _________
#   / / / / __ `/ __/ __ `/ ___/ __ \/ __ `/ ___/ _ \
#  / /_/ / /_/ / /_/ /_/ (__  ) /_/ / /_/ / /__/  __/
# /_____/\__,_/\__/\__,_/____/ .___/\__,_/\___/\___/
#                           /_/

@pytest.mark.asyncio
async def test_get_dataspaces() -> None:
    connection = ETPConnection()
    connection.is_connected = True

    get_dataspaces = Message.get_object_message(GetDataspaces(), msg_id=1,)

    answer = []
    async for m in (
        connection.handle_bytes_generator(get_dataspaces.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 1
    assert isinstance(answer[0].body, GetDataspacesResponse)
    assert len(answer[0].body.dataspaces) == 2

@pytest.mark.asyncio
async def test_delete_dataspaces() -> None:
    connection = ETPConnection()
    connection.is_connected = True

    delete_dataspaces = Message.get_object_message(DeleteDataspaces(uris={"a": "b", "c": "d"}), msg_id=1,)

    answer = []
    async for m in (
        connection.handle_bytes_generator(delete_dataspaces.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 1
    assert isinstance(answer[0].body, DeleteDataspacesResponse)
    assert len(answer[0].body.success) == 2
    assert list(answer[0].body.success.keys())[0] == "a"
    assert list(answer[0].body.success.keys())[1] == "c"

@pytest.mark.asyncio
async def test_put_dataspaces() -> None:
    connection = ETPConnection()
    connection.is_connected = True

    put_dataspaces = Message.get_object_message(PutDataspaces(dataspaces={
                        "a": Dataspace(uri="dataspace-default-0",
                            store_last_write=0,
                            store_created=0), 
                        "b": Dataspace(uri="dataspace-default-1",
                            store_last_write=0,
                            store_created=0)}), msg_id=1,)

    answer = []
    async for m in (
        connection.handle_bytes_generator(put_dataspaces.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 1
    assert isinstance(answer[0].body, PutDataspacesResponse)
    assert len(answer[0].body.success) == 2
    assert list(answer[0].body.success.keys())[0] == "a"
    assert list(answer[0].body.success.keys())[1] == "b"


#     ____  ___  _________         ___    ____  ____  _____  __
#    / __ \/   |/_  __/   |       /   |  / __ \/ __ \/   \ \/ /
#   / / / / /| | / / / /| |      / /| | / /_/ / /_/ / /| |\  /
#  / /_/ / ___ |/ / / ___ |     / ___ |/ _, _/ _, _/ ___ |/ /
# /_____/_/  |_/_/ /_/  |_|____/_/  |_/_/ |_/_/ |_/_/  |_/_/
#                        /_____/

@pytest.mark.asyncio
async def test_on_get_data_array_metadata() -> None:
    connection = ETPConnection()
    connection.is_connected = True

    get_data_array_metadata = Message.get_object_message(GetDataArrayMetadata(data_arrays={
                        "a": DataArrayIdentifier(uri="uri0", path_in_resource="myPath/a/b"),
                        "b": DataArrayIdentifier(uri="uri1", path_in_resource="myPath0/c/d"),
                        }), msg_id=1,)

    answer = []
    async for m in (
        connection.handle_bytes_generator(get_data_array_metadata.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 1
    assert isinstance(answer[0].body, GetDataArrayMetadataResponse)
    assert len(answer[0].body.array_metadata) == 2
    assert list(answer[0].body.array_metadata.keys())[0] == "a"
    assert list(answer[0].body.array_metadata.keys())[1] == "b"

@pytest.mark.asyncio
async def test_on_get_data_arrays() -> None:
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(GetDataArrays(data_arrays={
                        "a": DataArrayIdentifier(uri="uri0", path_in_resource="myPath/a/b"),
                        "b": DataArrayIdentifier(uri="uri1", path_in_resource="myPath0/c/d"),
                        }), msg_id=1,)

    answer = []
    async for m in (
        connection.handle_bytes_generator(get_data_arrays.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 1
    assert isinstance(answer[0].body, GetDataArraysResponse)
    assert len(answer[0].body.data_arrays) == 2
    assert list(answer[0].body.data_arrays.keys())[0] == "a"
    assert list(answer[0].body.data_arrays.keys())[1] == "b"

@pytest.mark.asyncio
async def test_on_get_data_subarrays() -> None:
    connection = ETPConnection()
    connection.is_connected = True

    get_data_subarrays = Message.get_object_message(GetDataSubarrays(data_subarrays={
                        "a": GetDataSubarraysType(
                                uid=DataArrayIdentifier(uri="uri0", path_in_resource="myPath/a/b"),
                                starts=[0,1,2],
                                counts=[10,15,20]),
                        "b": GetDataSubarraysType(
                                uid=DataArrayIdentifier(uri="uri0", path_in_resource="myPath/a/b"),
                                starts=[0,1,2],
                                counts=[10,15,20]),
                        }), msg_id=1,)

    answer = []
    async for m in (
        connection.handle_bytes_generator(get_data_subarrays.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 1
    assert isinstance(answer[0].body, GetDataSubarraysResponse)
    assert len(answer[0].body.data_subarrays) == 2
    assert list(answer[0].body.data_subarrays.keys())[0] == "a"
    assert list(answer[0].body.data_subarrays.keys())[1] == "b"

@pytest.mark.asyncio
async def test_on_put_data_arrays() -> None:
    connection = ETPConnection()
    connection.is_connected = True

    put_data_arrays = Message.get_object_message(PutDataArrays(data_arrays={
                        "a": PutDataArraysType(
                                uid=DataArrayIdentifier(uri="uri0", path_in_resource="myPath/a/b"),
                                array=DataArray(
                                        dimensions=[1],
                                        data=AnyArray(item=ArrayOfBoolean(values=[True, False]))
                                    ),
                                custom_data={},
                            ),
                        "b": PutDataArraysType(
                                uid=DataArrayIdentifier(uri="uri0", path_in_resource="myPath/a/b"),
                                array=DataArray(
                                        dimensions=[1],
                                        data=AnyArray(item=ArrayOfBoolean(values=[True, False]))
                                    ),
                                custom_data={},
                            ),
                        }), msg_id=1,)

    answer = []
    async for m in (
        connection.handle_bytes_generator(put_data_arrays.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 1
    assert isinstance(answer[0].body, PutDataArraysResponse)
    assert len(answer[0].body.success) == 2
    assert list(answer[0].body.success.keys())[0] == "a"
    assert list(answer[0].body.success.keys())[1] == "b"

@pytest.mark.asyncio
async def test_on_put_data_subarrays() -> None:
    connection = ETPConnection()
    connection.is_connected = True

    put_data_subarrays = Message.get_object_message(PutDataSubarrays(data_subarrays={
                        "a": PutDataSubarraysType(
                                uid=DataArrayIdentifier(uri="uri0", path_in_resource="myPath/a/b"),
                                data=AnyArray(item=ArrayOfBoolean(values=[True, False])),
                                starts=[2],
                                counts=[5],),
                        "b": PutDataSubarraysType(
                                uid=DataArrayIdentifier(uri="uri0", path_in_resource="myPath/a/b"),
                                data=AnyArray(item=ArrayOfBoolean(values=[True, False])),
                                starts=[2],
                                counts=[5],),
                        }), msg_id=1,)

    answer = []
    async for m in (
        connection.handle_bytes_generator(put_data_subarrays.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 1
    assert isinstance(answer[0].body, PutDataSubarraysResponse)
    assert len(answer[0].body.success) == 2
    assert list(answer[0].body.success.keys())[0] == "a"
    assert list(answer[0].body.success.keys())[1] == "b"

@pytest.mark.asyncio
async def test_on_put_uninitialized_data_arrays() -> None:
    connection = ETPConnection()
    connection.is_connected = True

    put_uninitialized_data_arrays = Message.get_object_message(PutUninitializedDataArrays(data_arrays={
                        "a": PutUninitializedDataArrayType(
                                uid=DataArrayIdentifier(uri="uri0", path_in_resource="myPath/a/b"),
                                metadata=DataArrayMetadata(
                                            dimensions=[1],
                                            transport_array_type=AnyArrayType.ARRAY_OF_BOOLEAN,
                                            logical_array_type=AnyLogicalArrayType.ARRAY_OF_BOOLEAN,
                                            store_last_write=0,
                                            store_created=0,
                                        )),
                        "b": PutUninitializedDataArrayType(
                                uid=DataArrayIdentifier(uri="uri0", path_in_resource="myPath/a/b"),
                                metadata=DataArrayMetadata(
                                            dimensions=[1],
                                            transport_array_type=AnyArrayType.ARRAY_OF_BOOLEAN,
                                            logical_array_type=AnyLogicalArrayType.ARRAY_OF_BOOLEAN,
                                            store_last_write=0,
                                            store_created=0,
                                        )),
                        }), msg_id=1,)

    answer = []
    async for m in (
        connection.handle_bytes_generator(put_uninitialized_data_arrays.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 1
    assert isinstance(answer[0].body, PutUninitializedDataArraysResponse)
    assert len(answer[0].body.success) == 2
    assert list(answer[0].body.success.keys())[0] == "a"
    assert list(answer[0].body.success.keys())[1] == "b"
