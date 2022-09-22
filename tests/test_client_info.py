import pytest

import uuid
from datetime import datetime

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


etp_version = Version(major=1, minor=2, revision=0, patch=0)

local_protocols = [
    SupportedProtocol(
        protocol=0,
        protocol_version=etp_version,
        role="server",
        protocol_capabilities={},
    ),
    SupportedProtocol(
        protocol=3,
        protocol_version=etp_version,
        role="store",
        protocol_capabilities={},
    ),
]

supported_objects = [
    SupportedDataObject(
        qualified_type="resqml20",
        data_object_capabilities={},
    )
]

supportedProtocolList = ETPConnection.get_supported_protocol_list()

my_open_session = OpenSession(
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
    session_id=uuid.uuid4(),
    current_date_time=int(datetime.utcnow().timestamp()),
    endpoint_capabilities={
        "MaxWebSocketFramePayloadSize": DataValue(item=666),
        "MaxWebSocketMessagePayloadSize": DataValue(item=10000000),
        "SupportsAlternateRequestUris": DataValue(item=True),
    },
    earliest_retained_change_time=int(datetime.utcnow().timestamp()),
)

my_request_session = RequestSession(
    application_name="WebStudio",
    application_version="1.2",
    client_instance_id=uuid.uuid4(),
    requested_protocols=local_protocols,
    supported_data_objects=supported_objects,
    supported_compression=["string"],
    supported_formats=["xml"],
    current_date_time=int(datetime.utcnow().timestamp()),
    endpoint_capabilities={
        "MaxWebSocketFramePayloadSize": DataValue(item=10000000),
        "MaxWebSocketMessagePayloadSize": DataValue(item=42),
        "SupportsAlternateRequestUris": DataValue(item=False),
    },
    earliest_retained_change_time=int(datetime.utcnow().timestamp()),
)


def test_negotiate_open_ession() -> None:
    client = ClientInfo()
    client.negotiate(my_open_session)

    assert client.endpoint_capabilities["MaxWebSocketFramePayloadSize"] == 666
    assert (
        client.endpoint_capabilities["MaxWebSocketMessagePayloadSize"] == 10000
    )
    assert client.endpoint_capabilities["SupportsAlternateRequestUris"]


def test_negotiate_request_session() -> None:
    client = ClientInfo()
    client.negotiate(my_request_session)

    assert (
        client.endpoint_capabilities["MaxWebSocketFramePayloadSize"] == 10000
    )
    assert client.endpoint_capabilities["MaxWebSocketMessagePayloadSize"] == 42
    assert not client.endpoint_capabilities["SupportsAlternateRequestUris"]
