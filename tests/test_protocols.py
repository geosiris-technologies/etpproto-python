# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

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
from etptypes.energistics.etp.v12.datatypes.object.context_info import (
    ContextInfo,
)

from etptypes.energistics.etp.v12.datatypes.object.context_scope_kind import (
    ContextScopeKind,
)
from etptypes.energistics.etp.v12.datatypes.object.relationship_kind import (
    RelationshipKind,
)
from etptypes.energistics.etp.v12.protocol.discovery_query.find_resources import (
    FindResources,
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

    get_dataspaces = Message.get_object_message(
        GetDataspaces(),
        msg_id=1,
    )

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

    delete_dataspaces = Message.get_object_message(
        DeleteDataspaces(uris={"a": "b", "c": "d"}),
        msg_id=1,
    )

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

    put_dataspaces = Message.get_object_message(
        PutDataspaces(
            dataspaces={
                "a": Dataspace(
                    uri="dataspace-default-0",
                    store_last_write=0,
                    store_created=0,
                ),
                "b": Dataspace(
                    uri="dataspace-default-1",
                    store_last_write=0,
                    store_created=0,
                ),
            }
        ),
        msg_id=1,
    )

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

    get_data_array_metadata = Message.get_object_message(
        GetDataArrayMetadata(
            data_arrays={
                "a": DataArrayIdentifier(
                    uri="uri0", path_in_resource="myPath/a/b"
                ),
                "b": DataArrayIdentifier(
                    uri="uri1", path_in_resource="myPath0/c/d"
                ),
            }
        ),
        msg_id=1,
    )

    answer = []
    async for m in (
        connection.handle_bytes_generator(
            get_data_array_metadata.encode_message()
        )
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

    get_data_arrays = Message.get_object_message(
        GetDataArrays(
            data_arrays={
                "a": DataArrayIdentifier(
                    uri="uri0", path_in_resource="myPath/a/b"
                ),
                "b": DataArrayIdentifier(
                    uri="uri1", path_in_resource="myPath0/c/d"
                ),
            }
        ),
        msg_id=1,
    )

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

    get_data_subarrays = Message.get_object_message(
        GetDataSubarrays(
            data_subarrays={
                "a": GetDataSubarraysType(
                    uid=DataArrayIdentifier(
                        uri="uri0", path_in_resource="myPath/a/b"
                    ),
                    starts=[0, 1, 2],
                    counts=[10, 15, 20],
                ),
                "b": GetDataSubarraysType(
                    uid=DataArrayIdentifier(
                        uri="uri0", path_in_resource="myPath/a/b"
                    ),
                    starts=[0, 1, 2],
                    counts=[10, 15, 20],
                ),
            }
        ),
        msg_id=1,
    )

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

    put_data_arrays = Message.get_object_message(
        PutDataArrays(
            data_arrays={
                "a": PutDataArraysType(
                    uid=DataArrayIdentifier(
                        uri="uri0", path_in_resource="myPath/a/b"
                    ),
                    array=DataArray(
                        dimensions=[1],
                        data=AnyArray(
                            item=ArrayOfBoolean(values=[True, False])
                        ),
                    ),
                    custom_data={},
                ),
                "b": PutDataArraysType(
                    uid=DataArrayIdentifier(
                        uri="uri0", path_in_resource="myPath/a/b"
                    ),
                    array=DataArray(
                        dimensions=[1],
                        data=AnyArray(
                            item=ArrayOfBoolean(values=[True, False])
                        ),
                    ),
                    custom_data={},
                ),
            }
        ),
        msg_id=1,
    )

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

    put_data_subarrays = Message.get_object_message(
        PutDataSubarrays(
            data_subarrays={
                "a": PutDataSubarraysType(
                    uid=DataArrayIdentifier(
                        uri="uri0", path_in_resource="myPath/a/b"
                    ),
                    data=AnyArray(item=ArrayOfBoolean(values=[True, False])),
                    starts=[2],
                    counts=[5],
                ),
                "b": PutDataSubarraysType(
                    uid=DataArrayIdentifier(
                        uri="uri0", path_in_resource="myPath/a/b"
                    ),
                    data=AnyArray(item=ArrayOfBoolean(values=[True, False])),
                    starts=[2],
                    counts=[5],
                ),
            }
        ),
        msg_id=1,
    )

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

    put_uninitialized_data_arrays = Message.get_object_message(
        PutUninitializedDataArrays(
            data_arrays={
                "a": PutUninitializedDataArrayType(
                    uid=DataArrayIdentifier(
                        uri="uri0", path_in_resource="myPath/a/b"
                    ),
                    metadata=DataArrayMetadata(
                        dimensions=[1],
                        transport_array_type=AnyArrayType.ARRAY_OF_BOOLEAN,
                        logical_array_type=AnyLogicalArrayType.ARRAY_OF_BOOLEAN,
                        store_last_write=0,
                        store_created=0,
                    ),
                ),
                "b": PutUninitializedDataArrayType(
                    uid=DataArrayIdentifier(
                        uri="uri0", path_in_resource="myPath/a/b"
                    ),
                    metadata=DataArrayMetadata(
                        dimensions=[1],
                        transport_array_type=AnyArrayType.ARRAY_OF_BOOLEAN,
                        logical_array_type=AnyLogicalArrayType.ARRAY_OF_BOOLEAN,
                        store_last_write=0,
                        store_created=0,
                    ),
                ),
            }
        ),
        msg_id=1,
    )

    answer = []
    async for m in (
        connection.handle_bytes_generator(
            put_uninitialized_data_arrays.encode_message()
        )
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


#     ____  _
#    / __ \(_)_____________ _   _____  _______  __
#   / / / / / ___/ ___/ __ \ | / / _ \/ ___/ / / /
#  / /_/ / (__  ) /__/ /_/ / |/ /  __/ /  / /_/ /
# /_____/_/____/\___/\____/|___/\___/_/   \__, /
#                                        /____/


@pytest.mark.asyncio
async def test_on_get_deleted_resources():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        GetDeletedResources(
            dataspace_uri="eml:///dataspace(my-dataspace)",
            delete_time_filter=1,
            data_object_types=["ChannelSet"],
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, GetDeletedResourcesResponse)
    assert len(answer[0].body.deleted_resources) == 2


@pytest.mark.asyncio
async def test_on_get_resources():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        GetResources(
            context=ContextInfo(
                uri="eml:///",
                depth=3,
                navigable_edges=RelationshipKind.BOTH,
            ),
            scope=ContextScopeKind.TARGETS,
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, GetResourcesResponse)
    assert len(answer[0].body.resources) == 2


#     ____  _                                      ____
#    / __ \(_)_____________ _   _____  _______  __/ __ \__  _____  _______  __
#   / / / / / ___/ ___/ __ \ | / / _ \/ ___/ / / / / / / / / / _ \/ ___/ / / /
#  / /_/ / (__  ) /__/ /_/ / |/ /  __/ /  / /_/ / /_/ / /_/ /  __/ /  / /_/ /
# /_____/_/____/\___/\____/|___/\___/_/   \__, /\___\_\__,_/\___/_/   \__, /
#                                        /____/                      /____/


@pytest.mark.asyncio
async def test_on_find_resources():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        FindResources(
            context=ContextInfo(
                uri="eml:///",
                depth=3,
                navigable_edges=RelationshipKind.BOTH,
            ),
            scope=ContextScopeKind.TARGETS,
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, FindResourcesResponse)
    assert len(answer[0].body.resources) == 2


#    ______                   _             ____  __      _           __  ____
#   / ____/________ _      __(_)___  ____ _/ __ \/ /_    (_)__  _____/ /_/ __ \__  _____  _______  __
#  / / __/ ___/ __ \ | /| / / / __ \/ __ `/ / / / __ \  / / _ \/ ___/ __/ / / / / / / _ \/ ___/ / / /
# / /_/ / /  / /_/ / |/ |/ / / / / / /_/ / /_/ / /_/ / / /  __/ /__/ /_/ /_/ / /_/ /  __/ /  / /_/ /
# \____/_/   \____/|__/|__/_/_/ /_/\__, /\____/_.___/_/ /\___/\___/\__/\___\_\__,_/\___/_/   \__, /
#                                 /____/           /___/                                    /____/


@pytest.mark.asyncio
async def test_on_find_parts():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        FindParts(
            uri="eml:///resqmlv2.TriangulatedSetRepresentation(260690d5-adc3-4f2c-b53e-2ff16345f52f)",
            format_="xml",
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, FindPartsResponse)
    assert (
        answer[0].body.uri
        == "eml:///resqmlv2.TriangulatedSetRepresentation(260690d5-adc3-4f2c-b53e-2ff16345f52f)"
    )
    assert answer[0].body.format_ == "xml"


#    _____ __                  ____
#   / ___// /_____  ________  / __ \__  _____  _______  __
#   \__ \/ __/ __ \/ ___/ _ \/ / / / / / / _ \/ ___/ / / /
#  ___/ / /_/ /_/ / /  /  __/ /_/ / /_/ /  __/ /  / /_/ /
# /____/\__/\____/_/   \___/\___\_\__,_/\___/_/   \__, /
#                                                /____/


@pytest.mark.asyncio
async def test_on_find_data_objects():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        FindDataObjects(
            context=ContextInfo(
                uri="eml:///",
                depth=3,
                navigable_edges=RelationshipKind.BOTH,
            ),
            scope=ContextScopeKind.TARGETS,
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, FindDataObjectsResponse)
    assert len(answer[0].body.data_objects) == 1
    assert answer[0].body.data_objects[0].format_ == "xml"


#    ______                   _             ____  __      _           __
#   / ____/________ _      __(_)___  ____ _/ __ \/ /_    (_)__  _____/ /_
#  / / __/ ___/ __ \ | /| / / / __ \/ __ `/ / / / __ \  / / _ \/ ___/ __/
# / /_/ / /  / /_/ / |/ |/ / / / / / /_/ / /_/ / /_/ / / /  __/ /__/ /_
# \____/_/   \____/|__/|__/_/_/ /_/\__, /\____/_.___/_/ /\___/\___/\__/
#                                 /____/           /___/


@pytest.mark.asyncio
async def test_on_delete_parts():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        DeleteParts(
            uri="",
            uids={
                "a": "x",
                "b": "y",
            },
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, DeletePartsResponse)


@pytest.mark.asyncio
async def test_on_get_change_annotations():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        GetChangeAnnotations(
            since_change_time=0,
            uris={
                "a": "x",
                "b": "y",
            },
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, GetChangeAnnotationsResponse)


@pytest.mark.asyncio
async def test_on_get_growing_data_objects_header():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        GetGrowingDataObjectsHeader(
            uris={
                "a": "x",
                "b": "y",
            },
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, GetGrowingDataObjectsHeaderResponse)


@pytest.mark.asyncio
async def test_on_get_parts():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        GetParts(
            uri="eml:///resqmlv2.TriangulatedSetRepresentation(260690d5-adc3-4f2c-b53e-2ff16345f52f)",
            uids={
                "a": "x",
                "b": "y",
            },
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, GetPartsResponse)


@pytest.mark.asyncio
async def test_on_get_parts_by_range():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        GetPartsByRange(
            uri="eml:///resqmlv2.TriangulatedSetRepresentation(260690d5-adc3-4f2c-b53e-2ff16345f52f)",
            index_interval=IndexInterval(
                start_index=IndexValue(),
                end_index=IndexValue(),
                uom="",
                depth_datum="",
            ),
            include_overlapping_intervals=True,
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, GetPartsByRangeResponse)


@pytest.mark.asyncio
async def test_on_get_parts_metadata():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        GetPartsMetadata(
            uris={
                "a": "x",
                "b": "y",
            },
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, GetPartsMetadataResponse)


@pytest.mark.asyncio
async def test_on_put_growing_data_objects_header():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        PutGrowingDataObjectsHeader(
            data_objects={
                "a": DataObject(
                    resource=Resource(
                        uri="eml:///witsml20.ChannelSet(2c0f6ef2-cc54-4104-8523-0f0fbaba3661)",
                        name="Test Name 0",
                        source_count=0,
                        target_count=0,
                        last_changed=0,
                        store_last_write=0,
                        store_created=0,
                        active_status=ActiveStatusKind.ACTIVE,
                    ),
                    blob_id=None,
                    format_="xml",
                    data="",
                ),
                "b": DataObject(
                    resource=Resource(
                        uri="eml:///witsml20.ChannelSet(2c0f6ef2-cc54-4104-8523-0f0fbaba3661)",
                        name="Test Name 1",
                        source_count=0,
                        target_count=0,
                        last_changed=0,
                        store_last_write=0,
                        store_created=0,
                        active_status=ActiveStatusKind.ACTIVE,
                    ),
                    blob_id=None,
                    format_="xml",
                    data="",
                ),
            }
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, PutGrowingDataObjectsHeaderResponse)


@pytest.mark.asyncio
async def test_on_put_parts():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        PutParts(
            uri="eml:///resqmlv2.TriangulatedSetRepresentation(260690d5-adc3-4f2c-b53e-2ff16345f52f)",
            parts={
                "a": ObjectPart(uid="x", data=b""),
                "b": ObjectPart(uid="x", data=b""),
            },
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, PutPartsResponse)


@pytest.mark.asyncio
async def test_on_replace_parts_by_range():
    connection = ETPConnection()
    connection.is_connected = True

    get_data_arrays = Message.get_object_message(
        ReplacePartsByRange(
            uri="eml:///resqmlv2.TriangulatedSetRepresentation(260690d5-adc3-4f2c-b53e-2ff16345f52f)",
            delete_interval=IndexInterval(
                start_index=IndexValue(),
                end_index=IndexValue(),
                uom="",
                depth_datum="",
            ),
            include_overlapping_intervals=True,
            parts=[
                ObjectPart(uid="x", data=b""),
                ObjectPart(uid="x", data=b""),
            ],
        ),
        msg_id=1,
    )

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
    assert isinstance(answer[0].body, ReplacePartsByRangeResponse)
