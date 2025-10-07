# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import datetime, timezone
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
import pytest
from etpproto.connection import ETPConnection
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

from etptypes.energistics.etp.v12.datatypes.object.context_info import (
    ContextInfo,
)
from etptypes.energistics.etp.v12.datatypes.object.relationship_kind import (
    RelationshipKind,
)
from etptypes.energistics.etp.v12.datatypes.object.context_scope_kind import (
    ContextScopeKind,
)

# -------------
from etpproto.protocols_new.core_handler import CoreHandler
from etpproto.client_info import ClientInfo


@ETPConnection.on()
class myCoreProtocol(CoreHandler):
    async def on_request_session(
        self,
        msg: RequestSession,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        print("RequestSession recieved, answer with OpenSession")
        supportedProtocolList = ETPConnection.get_supported_protocol_list()
        openSession = OpenSession(
            applicationName="etpproto",
            applicationVersion="1.0",
            serverInstanceId=uuid.uuid4(),
            supportedProtocols=supportedProtocolList,
            supportedDataObjects=[
                SupportedDataObject(
                    qualifiedType="resqml20",
                    dataObjectCapabilities={},
                )
            ],
            supportedCompression="string",
            supportedFormats=["xml"],
            sessionId=msg.client_instance_id,
            currentDateTime=int(datetime.now(timezone.utc).timestamp()),
            endpointCapabilities={},
            earliestRetainedChangeTime=int(
                datetime.now(timezone.utc).timestamp()
            ),
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
        yield

    async def on_ping(
        self,
        msg: Ping,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo],
    ) -> AsyncGenerator[Optional[Message], None]:
        print("#Core : Ping recieved")
        yield Message.get_object_message(
            Pong(currentDateTime=int(datetime.now(timezone.utc).timestamp())),
            correlation_id=msg_header.message_id,
        )

    async def on_pong(
        self,
        msg: Pong,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo],
    ) -> AsyncGenerator[Optional[Message], None]:
        print("#Core : Pong recieved")
        yield

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


etp_version = Version(major=1, minor=2, revision=0, patch=0)
local_protocols = [
    SupportedProtocol(
        protocol=0,
        protocolVersion=etp_version,
        role="server",
        protocolCapabilities={},
    ),
    SupportedProtocol(
        protocol=3,
        protocolVersion=etp_version,
        role="store",
        protocolCapabilities={},
    ),
]

supported_objects = [
    SupportedDataObject(
        qualifiedType="resqml20",
        dataObjectCapabilities={},  # DataObjectCapabilityKind.SUPPORTS_GET: DataValue(item=False)
    )
]


requestSession_msg = Message.get_object_message(
    RequestSession(
        applicationName="WebStudio",
        applicationVersion="1.2",
        clientInstanceId=uuid.uuid4(),
        requestedProtocols=local_protocols,
        supportedDataObjects=supported_objects,
        supportedCompression=["string"],
        supportedFormats=["xml"],
        currentDateTime=int(datetime.now(timezone.utc).timestamp()),
        endpointCapabilities={},
        earliestRetainedChangeTime=int(datetime.now(timezone.utc).timestamp()),
    ),
    msg_id=2,  # to test the correlation id
)

requestSession_msg_ask_acknowledge = Message.get_object_message(
    RequestSession(
        applicationName="WebStudio",
        applicationVersion="1.2",
        clientInstanceId=uuid.uuid4(),
        requestedProtocols=local_protocols,
        supportedDataObjects=supported_objects,
        supportedCompression=["string"],
        supportedFormats=["xml"],
        currentDateTime=int(datetime.now(timezone.utc).timestamp()),
        endpointCapabilities={},
        earliestRetainedChangeTime=int(datetime.now(timezone.utc).timestamp()),
    ),
    msg_id=1,
    message_flags=0x10,
)


supportedProtocolList = ETPConnection.get_supported_protocol_list()
openSession_msg = Message.get_object_message(
    OpenSession(
        applicationName="etpproto",
        applicationVersion="1.0",
        serverInstanceId=uuid.uuid4(),
        supportedProtocols=supportedProtocolList,
        supportedDataObjects=[
            SupportedDataObject(
                qualifiedType="resqml20",
                dataObjectCapabilities={},
            )
        ],
        supportedCompression="string",
        supportedFormats=["xml"],
        sessionId=uuid.uuid4(),
        currentDateTime=int(datetime.now(timezone.utc).timestamp()),
        endpointCapabilities={},
        earliestRetainedChangeTime=int(datetime.now(timezone.utc).timestamp()),
    ),
    msg_id=1,
)

getResources_msg = Message.get_object_message(
    GetResources(
        context=ContextInfo(
            uri="eml:///",
            depth=1,
            dataObjectTypes=[],
            navigableEdges=RelationshipKind.PRIMARY,
        ),
        scope=ContextScopeKind.SELF,
        countObjects=False,
        storeLastWriteFilter=None,
        activeStatusFilter=ActiveStatusKind.INACTIVE,
        includeEdges=False,
    ),
    msg_id=2,
)
closeSession_msg = Message.get_object_message(
    CloseSession(reason="On a fini et c'est tout!"), msg_id=3
)

resqml_obj_37166c33_3ebb_40ae_9bc6_1ab9693def60 = bytes(
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><resqml2:HorizonInterpretation xmlns:resqml2="http://www.energistics.org/energyml/data/resqmlv2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" schemaVersion="2.0" uuid="37166c33-3ebb-40ae-9bc6-1ab9693def60" xsi:type="resqml2:obj_HorizonInterpretation"> <eml:Citation xmlns:eml="http://www.energistics.org/energyml/data/commonv2" xsi:type="eml:Citation">        <eml:Title xsi:type="eml:DescriptionString">SnS</eml:Title>     <eml:Originator xsi:type="eml:NameString">Unknown</eml:Originator>      <eml:Creation xmlns:xsd="http://www.w3.org/2001/XMLSchema" xsi:type="xsd:dateTime">2014-12-18T10:47:47Z</eml:Creation>      <eml:Format xsi:type="eml:DescriptionString">SISMAGE</eml:Format>       <eml:Editor xsi:type="eml:NameString">Unknown</eml:Editor>  </eml:Citation> <resqml2:Domain xsi:type="resqml2:Domain">depth</resqml2:Domain>    <resqml2:InterpretedFeature xmlns:eml="http://www.energistics.org/energyml/data/commonv2" xsi:type="eml:DataObjectReference">       <eml:ContentType xmlns:xsd="http://www.w3.org/2001/XMLSchema" xsi:type="xsd:string">application/x-resqml+xml;version=2.0;type=obj_GeneticBoundaryFeature</eml:ContentType>      <eml:Title xsi:type="eml:DescriptionString">TopN2-2</eml:Title>     <eml:UUID xsi:type="eml:UuidString">85cad705-0228-4f31-905c-a22077316479</eml:UUID>     </resqml2:InterpretedFeature></resqml2:HorizonInterpretation>',
    encoding="utf-8",
)
resqml_obj_260690d5_adc3_4f2c_b53e_2ff16345f52f = bytes(
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><resqml2:HorizonInterpretation xmlns:resqml2="http://www.energistics.org/energyml/data/resqmlv2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" schemaVersion="2.0" uuid="260690d5-adc3-4f2c-b53e-2ff16345f52f" xsi:type="resqml2:obj_HorizonInterpretation">   <eml:Citation xmlns:eml="http://www.energistics.org/energyml/data/commonv2" xsi:type="eml:Citation">        <eml:Title xsi:type="eml:DescriptionString">SnS</eml:Title>     <eml:Originator xsi:type="eml:NameString">Unknown</eml:Originator>      <eml:Creation xmlns:xsd="http://www.w3.org/2001/XMLSchema" xsi:type="xsd:dateTime">2014-12-18T10:47:47Z</eml:Creation>      <eml:Format xsi:type="eml:DescriptionString">SISMAGE</eml:Format>       <eml:Editor xsi:type="eml:NameString">Unknown</eml:Editor>  </eml:Citation> <resqml2:Domain xsi:type="resqml2:Domain">depth</resqml2:Domain>    <resqml2:InterpretedFeature xmlns:eml="http://www.energistics.org/energyml/data/commonv2" xsi:type="eml:DataObjectReference">       <eml:ContentType xmlns:xsd="http://www.w3.org/2001/XMLSchema" xsi:type="xsd:string">application/x-resqml+xml;version=2.0;type=obj_GeneticBoundaryFeature</eml:ContentType>      <eml:Title xsi:type="eml:DescriptionString">TopEtive</eml:Title>        <eml:UUID xsi:type="eml:UuidString">f6e3f59f-8ca3-4497-838d-5db3015f2428</eml:UUID>     </resqml2:InterpretedFeature></resqml2:HorizonInterpretation>',
    encoding="utf-8",
)


@pytest.mark.asyncio
async def test_connection_state_2_distincts() -> None:
    connection_a = ETPConnection()
    connection_b = ETPConnection()

    assert not connection_a.is_connected
    assert not connection_b.is_connected

    async for m in connection_a._handle_message_generator(requestSession_msg):
        pass

    assert connection_a.is_connected
    assert not connection_b.is_connected

    async for m in connection_a._handle_message_generator(closeSession_msg):
        pass

    assert not connection_a.is_connected
    assert not connection_b.is_connected


if __name__ == "__main__":
    pytest.main()
