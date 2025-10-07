# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

import pytest

import uuid
from datetime import datetime, timezone

from etptypes.energistics.etp.v12.protocol.core.open_session import OpenSession
from etptypes.energistics.etp.v12.protocol.core.request_session import (
    RequestSession,
)
from etptypes.energistics.etp.v12.datatypes.supported_data_object import (
    SupportedDataObject,
)
from etptypes.energistics.etp.v12.datatypes.supported_protocol import (
    SupportedProtocol,
)
from etptypes.energistics.etp.v12.datatypes.version import Version
from etptypes.energistics.etp.v12.datatypes.data_value import DataValue

from etpproto.client_info import ClientInfo
from etpproto.connection import ETPConnection


from etptypes.energistics.etp.v12.datatypes.endpoint_capability_kind import (
    EndpointCapabilityKind,
)

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
        dataObjectCapabilities={},
    )
]

supportedProtocolList = ETPConnection.get_supported_protocol_list()

my_open_session = OpenSession(
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
    endpointCapabilities={
        EndpointCapabilityKind.MAX_WEB_SOCKET_FRAME_PAYLOAD_SIZE.value: DataValue(
            item=666
        ),
        EndpointCapabilityKind.MAX_WEB_SOCKET_MESSAGE_PAYLOAD_SIZE.value: DataValue(
            item=10000000
        ),
        EndpointCapabilityKind.SUPPORTS_ALTERNATE_REQUEST_URIS.value: DataValue(
            item=True
        ),
    },
    earliestRetainedChangeTime=int(datetime.now(timezone.utc).timestamp()),
)

my_request_session = RequestSession(
    applicationName="WebStudio",
    applicationVersion="1.2",
    clientInstanceId=uuid.uuid4(),
    requestedProtocols=local_protocols,
    supportedDataObjects=supported_objects,
    supportedCompression=["string"],
    supportedFormats=["xml"],
    currentDateTime=int(datetime.now(timezone.utc).timestamp()),
    endpointCapabilities={
        EndpointCapabilityKind.MAX_WEB_SOCKET_FRAME_PAYLOAD_SIZE.value: DataValue(
            item=10000000
        ),
        EndpointCapabilityKind.MAX_WEB_SOCKET_MESSAGE_PAYLOAD_SIZE.value: DataValue(
            item=42
        ),
        EndpointCapabilityKind.SUPPORTS_ALTERNATE_REQUEST_URIS.value: DataValue(
            item=False
        ),
    },
    earliestRetainedChangeTime=int(datetime.now(timezone.utc).timestamp()),
)


def test_negotiate_open_ession() -> None:
    client = ClientInfo()
    client.negotiate(my_open_session)

    assert (
        client.endpoint_capabilities[
            EndpointCapabilityKind.MAX_WEB_SOCKET_FRAME_PAYLOAD_SIZE.value
        ]
        == 666
    )
    assert (
        client.endpoint_capabilities[
            EndpointCapabilityKind.MAX_WEB_SOCKET_MESSAGE_PAYLOAD_SIZE.value
        ]
        == 10000
    )
    assert client.endpoint_capabilities[
        EndpointCapabilityKind.SUPPORTS_ALTERNATE_REQUEST_URIS.value
    ]


def test_negotiate_request_session() -> None:
    client = ClientInfo()
    client.negotiate(my_request_session)

    assert (
        client.endpoint_capabilities[
            EndpointCapabilityKind.MAX_WEB_SOCKET_FRAME_PAYLOAD_SIZE.value
        ]
        == 10000
    )
    assert (
        client.endpoint_capabilities[
            EndpointCapabilityKind.MAX_WEB_SOCKET_MESSAGE_PAYLOAD_SIZE.value
        ]
        == 42
    )
    assert not client.endpoint_capabilities[
        EndpointCapabilityKind.SUPPORTS_ALTERNATE_REQUEST_URIS.value
    ]
