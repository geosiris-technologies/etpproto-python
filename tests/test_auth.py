# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0
import asyncio
import pytest
import base64

from etptypes.energistics.etp.v12.protocol.core.protocol_exception import (
    ProtocolException,
)

from etpproto.error import (
    AuthorizationRequired,
)

try:
    from .server_protocol_example import *
except Exception:
    from server_protocol_example import *

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
        data_object_capabilities={},  # DataObjectCapabilityKind.SUPPORTS_GET: DataValue(item=False)
    )
]

requestSession_msg = Message.get_object_message(
    RequestSession(
        application_name="WebStudio",
        application_version="1.2",
        client_instance_id=uuid.uuid4(),
        requested_protocols=local_protocols,
        supported_data_objects=supported_objects,
        supported_compression=["string"],
        supported_formats=["xml"],
        current_date_time=int(datetime.utcnow().timestamp()),
        endpoint_capabilities={},
        earliest_retained_change_time=int(datetime.utcnow().timestamp()),
    ),
    msg_id=2,  # to test the correlation id
)


def basic_auth_encode(username, password):
    assert ":" not in username
    user_pass = f"{username}:{password}"
    basic_credentials = base64.b64encode(user_pass.encode()).decode()
    return "Basic " + basic_credentials


@pytest.mark.asyncio
async def test_connection_authorize_msg_response() -> None:
    connection = ETPConnection(auth_required=True)
    auth_msg = Message.get_object_message(
        Authorize(authorization="basic aaa", supplemental_authorization={}),
        msg_id=1,
    )
    answer = []
    async for m in connection.handle_bytes_generator(
        auth_msg.encode_message()
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 1
    assert answer[0].is_final_msg()
    assert isinstance(answer[0].body, AuthorizeResponse)


@pytest.mark.asyncio
async def test_connection_authorize_msg_basic_correct() -> None:
    username = "usernameTest"
    password = "passwordTest"

    connection = ETPConnection(auth_required=True)
    auth_msg = Message.get_object_message(
        Authorize(
            authorization=basic_auth_encode(username, password),
            supplemental_authorization={},
        ),
        msg_id=1,
    )
    answer = []
    async for m in connection.handle_bytes_generator(
        auth_msg.encode_message()
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert isinstance(answer[0].body, AuthorizeResponse)
    assert answer[0].body.success


@pytest.mark.asyncio
async def test_connection_authorize_msg_basic_wrong_username() -> None:
    username = "notAGoodUsername"
    password = "passwordTest"

    connection = ETPConnection(auth_required=True)
    auth_msg = Message.get_object_message(
        Authorize(
            authorization=basic_auth_encode(username, password),
            supplemental_authorization={},
        ),
        msg_id=1,
    )
    answer = []
    async for m in connection.handle_bytes_generator(
        auth_msg.encode_message()
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert isinstance(answer[0].body, AuthorizeResponse)
    assert not answer[0].body.success


@pytest.mark.asyncio
async def test_connection_authorize_msg_basic_wrong_password() -> None:
    username = "usernameTest"
    password = "notAGoodPassword"

    connection = ETPConnection(auth_required=True)
    auth_msg = Message.get_object_message(
        Authorize(
            authorization=basic_auth_encode(username, password),
            supplemental_authorization={},
        ),
        msg_id=1,
    )
    answer = []
    async for m in connection.handle_bytes_generator(
        auth_msg.encode_message()
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert isinstance(answer[0].body, AuthorizeResponse)
    assert not answer[0].body.success


@pytest.mark.asyncio
async def test_connection_not_auth_error() -> None:
    connection = ETPConnection(auth_required=True)

    answer = []
    async for m in connection.handle_bytes_generator(
        requestSession_msg.encode_message()
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert isinstance(answer[0].body, ProtocolException)
    assert answer[0].body.error.code == AuthorizationRequired().code
