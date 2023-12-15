# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import datetime
from typing import AsyncGenerator, Optional, Union
import base64

from etptypes.energistics.etp.v12.datatypes.contact import Contact
from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader
from etptypes.energistics.etp.v12.datatypes.server_capabilities import (
    ServerCapabilities,
)

from etptypes.energistics.etp.v12.datatypes.supported_data_object import (
    SupportedDataObject,
)
from etptypes.energistics.etp.v12.datatypes.supported_protocol import (
    SupportedProtocol,
)
from etptypes.energistics.etp.v12.datatypes.uuid import Uuid
from etptypes.energistics.etp.v12.datatypes.version import Version
from etptypes.energistics.etp.v12.protocol.core.close_session import (
    CloseSession,
)
from etptypes.energistics.etp.v12.protocol.core.open_session import OpenSession
from etptypes.energistics.etp.v12.protocol.core.ping import Ping
from etptypes.energistics.etp.v12.protocol.core.pong import Pong
from etptypes.energistics.etp.v12.protocol.core.request_session import (
    RequestSession,
)
from etptypes.energistics.etp.v12.protocol.core.authorize import Authorize
from etptypes.energistics.etp.v12.protocol.core.authorize_response import (
    AuthorizeResponse,
)

# =========================== DATASPACE PROTOCOL
from etptypes.energistics.etp.v12.datatypes.object.dataspace import Dataspace
from etptypes.energistics.etp.v12.protocol.dataspace.get_dataspaces import (
    GetDataspaces,
)
from etptypes.energistics.etp.v12.protocol.dataspace.get_dataspaces_response import (
    GetDataspacesResponse,
)

from etptypes.energistics.etp.v12.protocol.dataspace.put_dataspaces import (
    PutDataspaces,
)
from etptypes.energistics.etp.v12.protocol.dataspace.put_dataspaces_response import (
    PutDataspacesResponse,
)

from etptypes.energistics.etp.v12.protocol.dataspace.delete_dataspaces import (
    DeleteDataspaces,
)
from etptypes.energistics.etp.v12.protocol.dataspace.delete_dataspaces_response import (
    DeleteDataspacesResponse,
)


from etptypes.energistics.etp.v12.protocol.store.get_data_objects_response import (
    GetDataObjectsResponse,
)

# =========================== DATA_ARRAY PROTOCOL
from etptypes.energistics.etp.v12.datatypes.any_logical_array_type import (
    AnyLogicalArrayType,
)
from etptypes.energistics.etp.v12.datatypes.any_array_type import AnyArrayType

from etptypes.energistics.etp.v12.datatypes.data_array_types.data_array import (
    DataArray,
)
from etptypes.energistics.etp.v12.datatypes.array_of_boolean import (
    ArrayOfBoolean,
)
from etptypes.energistics.etp.v12.datatypes.array_of_float import (
    ArrayOfFloat,
)
from etptypes.energistics.etp.v12.datatypes.data_array_types.data_array_metadata import (
    DataArrayMetadata,
)
from etpproto.protocols.data_array import *
from etptypes.energistics.etp.v12.datatypes.any_array import AnyArray

# =========================== DISCOVERY PROTOCOL
from etptypes.energistics.etp.v12.protocol.discovery.get_deleted_resources import (
    GetDeletedResources,
)
from etptypes.energistics.etp.v12.protocol.discovery.get_deleted_resources_response import (
    GetDeletedResourcesResponse,
)
from etptypes.energistics.etp.v12.protocol.discovery.get_resources import (
    GetResources,
)
from etptypes.energistics.etp.v12.protocol.discovery.get_resources_edges_response import (
    GetResourcesEdgesResponse,
)
from etptypes.energistics.etp.v12.protocol.discovery.get_resources_response import (
    GetResourcesResponse,
)
from etptypes.energistics.etp.v12.datatypes.object.resource import Resource
from etptypes.energistics.etp.v12.datatypes.object.active_status_kind import (
    ActiveStatusKind,
)
from etptypes.energistics.etp.v12.datatypes.object.deleted_resource import (
    DeletedResource,
)

# =========================== DISCOVERY QUERY PROTOCOL
from etptypes.energistics.etp.v12.protocol.discovery_query.find_resources_response import (
    FindResourcesResponse,
)
from etptypes.energistics.etp.v12.protocol.discovery_query.find_resources import (
    FindResources,
)

# =========================== GROWING OBJECT PROTOCOL
from etptypes.energistics.etp.v12.protocol.growing_object_query.find_parts import (
    FindParts,
)
from etptypes.energistics.etp.v12.protocol.growing_object_query.find_parts_response import (
    FindPartsResponse,
)

# =========================== STORE QUERY PROTOCOL
from etptypes.energistics.etp.v12.protocol.store_query.find_data_objects import (
    FindDataObjects,
)
from etptypes.energistics.etp.v12.protocol.store_query.find_data_objects_response import (
    FindDataObjectsResponse,
)
from etptypes.energistics.etp.v12.datatypes.object.data_object import (
    DataObject,
)

# =========================== GROWING OBJECT PROTOCOL
from etptypes.energistics.etp.v12.protocol.growing_object.delete_parts import (
    DeleteParts,
)
from etptypes.energistics.etp.v12.protocol.growing_object.delete_parts_response import (
    DeletePartsResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_change_annotations import (
    GetChangeAnnotations,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_change_annotations_response import (
    GetChangeAnnotationsResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_growing_data_objects_header import (
    GetGrowingDataObjectsHeader,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_growing_data_objects_header_response import (
    GetGrowingDataObjectsHeaderResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts import (
    GetParts,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_by_range import (
    GetPartsByRange,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_by_range_response import (
    GetPartsByRangeResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_metadata import (
    GetPartsMetadata,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_metadata_response import (
    GetPartsMetadataResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_response import (
    GetPartsResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.put_growing_data_objects_header import (
    PutGrowingDataObjectsHeader,
)
from etptypes.energistics.etp.v12.protocol.growing_object.put_growing_data_objects_header_response import (
    PutGrowingDataObjectsHeaderResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.put_parts import (
    PutParts,
)
from etptypes.energistics.etp.v12.protocol.growing_object.put_parts_response import (
    PutPartsResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.replace_parts_by_range import (
    ReplacePartsByRange,
)
from etptypes.energistics.etp.v12.protocol.growing_object.replace_parts_by_range_response import (
    ReplacePartsByRangeResponse,
)
from etptypes.energistics.etp.v12.datatypes.object.change_response_info import (
    ChangeResponseInfo,
)
from etptypes.energistics.etp.v12.datatypes.object.object_part import (
    ObjectPart,
)
from etptypes.energistics.etp.v12.datatypes.object.parts_metadata_info import (
    PartsMetadataInfo,
)
from etptypes.energistics.etp.v12.datatypes.channel_data.index_metadata_record import (
    IndexMetadataRecord,
)
from etptypes.energistics.etp.v12.datatypes.channel_data.channel_index_kind import (
    ChannelIndexKind,
)

from etptypes.energistics.etp.v12.datatypes.object.index_interval import (
    IndexInterval,
)

from etptypes.energistics.etp.v12.datatypes.channel_data.index_direction import (
    IndexDirection,
)
from etptypes.energistics.etp.v12.datatypes.index_value import IndexValue

# ===========================

from etpproto.client_info import ClientInfo
from etpproto.connection import (
    CommunicationProtocol,
    ConnectionType,
    ETPConnection,
)
from etpproto.error import NotSupportedError
from etpproto.messages import Message
from etpproto.protocols.core import CoreHandler
from etpproto.protocols.store import StoreHandler
from etpproto.protocols.dataspace import DataspaceHandler
from etpproto.protocols.discovery import DiscoveryHandler
from etpproto.protocols.discovery_query import DiscoveryQueryHandler
from etpproto.protocols.growing_object_query import GrowingObjectQueryHandler
from etpproto.protocols.store_query import StoreQueryHandler
from etpproto.protocols.growing_object import GrowingObjectHandler

#    ______                                    __                   __
#   / ____/___  ________     ____  _________  / /_____  _________  / /
#  / /   / __ \/ ___/ _ \   / __ \/ ___/ __ \/ __/ __ \/ ___/ __ \/ /
# / /___/ /_/ / /  /  __/  / /_/ / /  / /_/ / /_/ /_/ / /__/ /_/ / /
# \____/\____/_/   \___/  / .___/_/   \____/\__/\____/\___/\____/_/
#                        /_/


@ETPConnection.on(CommunicationProtocol.CORE)
class myCoreProtocol(CoreHandler):
    async def on_request_session(
        self,
        msg: RequestSession,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo],
    ) -> AsyncGenerator[Optional[Message], None]:
        print("RequestSession recieved, answer with OpenSession")
        supportedProtocolList = ETPConnection.get_supported_protocol_list()
        openSession = OpenSession(
            application_name="etpproto",
            application_version="1.0",
            server_instance_id=uuid.uuid4(),
            supported_protocols=supportedProtocolList,
            supported_data_objects=[
                SupportedDataObject(
                    qualified_type="resqml20",
                    data_object_capabilities={},
                )
            ],
            supported_compression="string",
            supported_formats=["xml"],
            session_id=msg.client_instance_id,
            current_date_time=int(datetime.utcnow().timestamp()),
            endpoint_capabilities={},
            earliest_retained_change_time=int(datetime.utcnow().timestamp()),
        )
        # TODO: Attention ici le msgId est mauvais il faudra le changer a posteriori
        yield Message.get_object_message(
            openSession, correlation_id=msg_header.message_id
        )

    async def on_close_session(
        self,
        msg: CloseSession,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo],
    ) -> AsyncGenerator[Optional[Message], None]:
        print("closing")

    async def on_ping(
        self,
        msg: Ping,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo],
    ) -> AsyncGenerator[Optional[Message], None]:
        print("#Core : Ping recieved")
        yield Message.get_object_message(
            Pong(current_date_time=int(datetime.utcnow().timestamp())),
            correlation_id=msg_header.message_id,
        )

    async def on_pong(
        self,
        msg: Pong,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo],
    ) -> AsyncGenerator[Optional[Message], None]:
        print("#Core : Pong recieved")

    async def on_authorize(
        self,
        msg: Authorize,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        username = "usernameTest"
        password = "passwordTest"
        success = False
        try:
            scheme, credentials = msg.authorization.split()
            dec_username = None
            dec_password = None
            if scheme.lower() == "basic":
                decoded = base64.b64decode(credentials).decode("ascii")
                dec_username, _, dec_password = decoded.partition(":")
            success = dec_username == username and dec_password == password
        except Exception:
            pass
        yield Message.get_object_message(
            AuthorizeResponse(success=success, challenges=[]),
            correlation_id=msg_header.message_id,
        )

    async def on_authorize_response(
        self,
        msg: AuthorizeResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_open_session(
        self,
        msg: OpenSession,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield None


@ETPConnection.on(CommunicationProtocol.STORE)
class myStoreProtocol(StoreHandler):
    async def on_get_data_objects_response(
        self,
        msg: GetDataObjectsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        print("GetDataObjectsResponse recieved")
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )


@ETPConnection.on(CommunicationProtocol.DATASPACE)
class myDataspaceProtocol(DataspaceHandler):
    async def on_delete_dataspaces(
        self,
        msg: DeleteDataspaces,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            DeleteDataspacesResponse(
                success={k: True for k, v in msg.uris.items()}
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_get_dataspaces(
        self,
        msg: GetDataspaces,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            GetDataspacesResponse(
                dataspaces=[
                    Dataspace(
                        uri="dataspace-default-0",
                        store_last_write=0,
                        store_created=0,
                    ),
                    Dataspace(
                        uri="dataspace-default-1",
                        store_last_write=0,
                        store_created=0,
                    ),
                ]
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_put_dataspaces(
        self,
        msg: PutDataspaces,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            PutDataspacesResponse(
                success={k: True for k, v in msg.dataspaces.items()}
            ),
            correlation_id=msg_header.message_id,
        )


#     ____  ___  _________         ___    ____  ____  _____  __
#    / __ \/   |/_  __/   |       /   |  / __ \/ __ \/   \ \/ /
#   / / / / /| | / / / /| |      / /| | / /_/ / /_/ / /| |\  /
#  / /_/ / ___ |/ / / ___ |     / ___ |/ _, _/ _, _/ ___ |/ /
# /_____/_/  |_/_/ /_/  |_|____/_/  |_/_/ |_/_/ |_/_/  |_/_/
#                        /_____/


@ETPConnection.on(CommunicationProtocol.DATA_ARRAY)
class myDataArrayProtocol(DataArrayHandler):
    async def on_get_data_array_metadata(
        self,
        msg: GetDataArrayMetadata,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            GetDataArrayMetadataResponse(
                array_metadata={
                    k: DataArrayMetadata(
                        dimensions=[1],
                        transport_array_type=AnyArrayType.ARRAY_OF_BOOLEAN,
                        logical_array_type=AnyLogicalArrayType.ARRAY_OF_BOOLEAN,
                        store_last_write=0,
                        store_created=0,
                    )
                    for k, v in msg.data_arrays.items()
                }
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_get_data_arrays(
        self,
        msg: GetDataArrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            GetDataArraysResponse(
                data_arrays={
                    k: DataArray(
                        dimensions=[1],
                        data=AnyArray(item=ArrayOfFloat(values=[3.2, 1.0])),
                    )
                    for k, v in msg.data_arrays.items()
                }
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_get_data_subarrays(
        self,
        msg: GetDataSubarrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            GetDataSubarraysResponse(
                data_subarrays={
                    k: DataArray(
                        dimensions=[1],
                        data=AnyArray(item=ArrayOfFloat(values=[3.2, 1.0])),
                    )
                    for k, v in msg.data_subarrays.items()
                }
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_put_data_arrays(
        self,
        msg: PutDataArrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            PutDataArraysResponse(
                success={k: True for k, v in msg.data_arrays.items()}
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_put_data_subarrays(
        self,
        msg: PutDataSubarrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            PutDataSubarraysResponse(
                success={k: True for k, v in msg.data_subarrays.items()}
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_put_uninitialized_data_arrays(
        self,
        msg: PutUninitializedDataArrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            PutUninitializedDataArraysResponse(
                success={k: True for k, v in msg.data_arrays.items()}
            ),
            correlation_id=msg_header.message_id,
        )


#     ____  _
#    / __ \(_)_____________ _   _____  _______  __
#   / / / / / ___/ ___/ __ \ | / / _ \/ ___/ / / /
#  / /_/ / (__  ) /__/ /_/ / |/ /  __/ /  / /_/ /
# /_____/_/____/\___/\____/|___/\___/_/   \__, /
#                                        /____/


@ETPConnection.on(CommunicationProtocol.DISCOVERY)
class MyDiscoveryHandler(DiscoveryHandler):
    async def on_get_deleted_resources(
        self,
        msg: GetDeletedResources,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            GetDeletedResourcesResponse(
                deleted_resources=[
                    DeletedResource(
                        uri="eml:///witsml20.ChannelSet(2c0f6ef2-cc54-4104-8523-0f0fbaba3661)",
                        deleted_time=0,
                    ),
                    DeletedResource(
                        uri="eml:///witsml20.ChannelSet(2c0f6ef2-cc54-4104-8523-0f0fbaba3661)",
                        deleted_time=0,
                    ),
                ]
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_get_resources(
        self,
        msg: GetResources,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            GetResourcesResponse(
                resources=[
                    Resource(
                        uri="eml:///witsml20.ChannelSet(2c0f6ef2-cc54-4104-8523-0f0fbaba3661)",
                        name="Test Name 0",
                        source_count=0,
                        target_count=0,
                        last_changed=0,
                        store_last_write=0,
                        store_created=0,
                        active_status=ActiveStatusKind.ACTIVE,
                    ),
                    Resource(
                        uri="eml:///witsml20.ChannelSet(2c0f6ef2-cc54-4104-8523-0f0fbaba3661)",
                        name="Test Name 0",
                        source_count=0,
                        target_count=0,
                        last_changed=0,
                        store_last_write=0,
                        store_created=0,
                        active_status=ActiveStatusKind.ACTIVE,
                    ),
                ]
            ),
            correlation_id=msg_header.message_id,
        )


#     ____  _                                      ____
#    / __ \(_)_____________ _   _____  _______  __/ __ \__  _____  _______  __
#   / / / / / ___/ ___/ __ \ | / / _ \/ ___/ / / / / / / / / / _ \/ ___/ / / /
#  / /_/ / (__  ) /__/ /_/ / |/ /  __/ /  / /_/ / /_/ / /_/ /  __/ /  / /_/ /
# /_____/_/____/\___/\____/|___/\___/_/   \__, /\___\_\__,_/\___/_/   \__, /
#                                        /____/                      /____/


@ETPConnection.on(CommunicationProtocol.DISCOVERY_QUERY)
class MyDiscoveryQueryHandler(DiscoveryQueryHandler):
    async def on_find_resources(
        self,
        msg: FindResources,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            FindResourcesResponse(
                server_sort_order="",
                resources=[
                    Resource(
                        uri="eml:///witsml20.ChannelSet(2c0f6ef2-cc54-4104-8523-0f0fbaba3661)",
                        name="Test Name 0",
                        source_count=0,
                        target_count=0,
                        last_changed=0,
                        store_last_write=0,
                        store_created=0,
                        active_status=ActiveStatusKind.ACTIVE,
                    ),
                    Resource(
                        uri="eml:///witsml20.ChannelSet(2c0f6ef2-cc54-4104-8523-0f0fbaba3661)",
                        name="Test Name 0",
                        source_count=0,
                        target_count=0,
                        last_changed=0,
                        store_last_write=0,
                        store_created=0,
                        active_status=ActiveStatusKind.ACTIVE,
                    ),
                ],
            ),
            correlation_id=msg_header.message_id,
        )


#    ______                   _             ____  __      _           __  ____
#   / ____/________ _      __(_)___  ____ _/ __ \/ /_    (_)__  _____/ /_/ __ \__  _____  _______  __
#  / / __/ ___/ __ \ | /| / / / __ \/ __ `/ / / / __ \  / / _ \/ ___/ __/ / / / / / / _ \/ ___/ / / /
# / /_/ / /  / /_/ / |/ |/ / / / / / /_/ / /_/ / /_/ / / /  __/ /__/ /_/ /_/ / /_/ /  __/ /  / /_/ /
# \____/_/   \____/|__/|__/_/_/ /_/\__, /\____/_.___/_/ /\___/\___/\__/\___\_\__,_/\___/_/   \__, /
#                                 /____/           /___/                                    /____/


@ETPConnection.on(CommunicationProtocol.GROWING_OBJECT_QUERY)
class MyGrowingObjectQueryHandler(GrowingObjectQueryHandler):
    async def on_find_parts(
        self,
        msg: FindParts,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            FindPartsResponse(
                uri=msg.uri,
                server_sort_order="",
                format_=msg.format_,
                parts=[],
            ),
            correlation_id=msg_header.message_id,
        )


#    _____ __                  ____
#   / ___// /_____  ________  / __ \__  _____  _______  __
#   \__ \/ __/ __ \/ ___/ _ \/ / / / / / / _ \/ ___/ / / /
#  ___/ / /_/ /_/ / /  /  __/ /_/ / /_/ /  __/ /  / /_/ /
# /____/\__/\____/_/   \___/\___\_\__,_/\___/_/   \__, /
#                                                /____/


@ETPConnection.on(CommunicationProtocol.STORE_QUERY)
class MyStoreQueryHandler(StoreQueryHandler):
    async def on_find_data_objects(
        self,
        msg: FindDataObjects,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            FindDataObjectsResponse(
                server_sort_order="",
                data_objects=[
                    DataObject(
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
                ],
            ),
            correlation_id=msg_header.message_id,
        )


#    ______                   _             ____  __      _           __
#   / ____/________ _      __(_)___  ____ _/ __ \/ /_    (_)__  _____/ /_
#  / / __/ ___/ __ \ | /| / / / __ \/ __ `/ / / / __ \  / / _ \/ ___/ __/
# / /_/ / /  / /_/ / |/ |/ / / / / / /_/ / /_/ / /_/ / / /  __/ /__/ /_
# \____/_/   \____/|__/|__/_/_/ /_/\__, /\____/_.___/_/ /\___/\___/\__/
#                                 /____/           /___/


@ETPConnection.on(CommunicationProtocol.GROWING_OBJECT)
class MyGrowingObjectHandler(GrowingObjectHandler):
    async def on_delete_parts(
        self,
        msg: DeleteParts,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            DeletePartsResponse(
                success={k: True for k, v in msg.uids.items()}
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_get_change_annotations(
        self,
        msg: GetChangeAnnotations,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            GetChangeAnnotationsResponse(changes={}),
            correlation_id=msg_header.message_id,
        )

    async def on_get_growing_data_objects_header(
        self,
        msg: GetGrowingDataObjectsHeader,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            GetGrowingDataObjectsHeaderResponse(
                data_objects={
                    k: DataObject(
                        resource=Resource(
                            uri=v,
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
                    )
                    for k, v in msg.uris.items()
                }
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_get_parts(
        self,
        msg: GetParts,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            GetPartsResponse(
                uri=msg.uri,
                parts={
                    k: ObjectPart(uid=k, data=b"") for k, v in msg.uids.items()
                },
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_get_parts_by_range(
        self,
        msg: GetPartsByRange,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            GetPartsByRangeResponse(
                uri=msg.uri,
                parts=[ObjectPart(uid="a", data=b"")],
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_get_parts_metadata(
        self,
        msg: GetPartsMetadata,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            GetPartsMetadataResponse(
                metadata={
                    k: PartsMetadataInfo(
                        uri=k,
                        name="My name " + k,
                        index=IndexMetadataRecord(
                            interval=IndexInterval(
                                start_index=IndexValue(),
                                end_index=IndexValue(),
                                uom="",
                                depth_datum="",
                            ),
                            uom="",
                            index_property_kind_uri="",
                            index_kind=ChannelIndexKind.PRESSURE,
                            direction=IndexDirection.INCREASING,
                        ),
                    )
                    for k, v in msg.uris.items()
                },
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_put_growing_data_objects_header(
        self,
        msg: PutGrowingDataObjectsHeader,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            PutGrowingDataObjectsHeaderResponse(
                success={k: True for k, v in msg.data_objects.items()}
            ),
            correlation_id=msg_header.message_id,
        )

    async def on_put_parts(
        self,
        msg: PutParts,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            PutPartsResponse(success={k: True for k, v in msg.parts.items()}),
            correlation_id=msg_header.message_id,
        )

    async def on_replace_parts_by_range(
        self,
        msg: ReplacePartsByRange,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield Message.get_object_message(
            ReplacePartsByRangeResponse(),
            correlation_id=msg_header.message_id,
        )
