# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncGenerator, Optional, Protocol, Union

from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader
from etptypes.energistics.etp.v12.protocol.core.acknowledge import Acknowledge
from etptypes.energistics.etp.v12.protocol.core.authorize import Authorize
from etptypes.energistics.etp.v12.protocol.core.authorize_response import (
    AuthorizeResponse,
)
from etptypes.energistics.etp.v12.protocol.core.close_session import (
    CloseSession,
)
from etptypes.energistics.etp.v12.protocol.core.open_session import OpenSession
from etptypes.energistics.etp.v12.protocol.core.ping import Ping
from etptypes.energistics.etp.v12.protocol.core.pong import Pong
from etptypes.energistics.etp.v12.protocol.core.protocol_exception import (
    ProtocolException,
)
from etptypes.energistics.etp.v12.protocol.core.request_session import (
    RequestSession,
)

from etpproto.client_info import ClientInfo
from etpproto.error import InvalidMessageTypeError, NotSupportedError
from etpproto.messages import Message
from etpproto.utils import snake_case


class CoreHandler(Protocol):
    async def on_acknowledge(
        self,
        msg: Acknowledge,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_authorize(
        self,
        msg: Authorize,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
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

    async def on_close_session(
        self,
        msg: CloseSession,
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
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_ping(
        self,
        msg: Ping,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_pong(
        self,
        msg: Pong,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_protocol_exception(
        self,
        msg: ProtocolException,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_request_session(
        self,
        msg: RequestSession,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def handle_message(
        self,
        etp_object: object,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        handling_func = getattr(
            self, "on_" + snake_case(type(etp_object).__name__)
        )
        if handling_func is not None:
            async for handled in handling_func(
                msg=etp_object,
                msg_header=msg_header,
                client_info=client_info,
            ):
                yield handled

        else:
            raise InvalidMessageTypeError()
