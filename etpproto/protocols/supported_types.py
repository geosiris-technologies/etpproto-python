
# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0
from typing import AsyncGenerator, Optional, Union, ClassVar
from dataclasses import dataclass

from etpproto.client_info import ClientInfo
from etpproto.error import InvalidMessageTypeError, NotSupportedError
from etpproto.messages import Message
from etpproto.connection import Protocol
from etpproto.utils import snake_case

from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader

from etptypes.energistics.etp.v12.protocol.supported_types.get_supported_types import GetSupportedTypes
from etptypes.energistics.etp.v12.protocol.supported_types.get_supported_types_response import GetSupportedTypesResponse

@dataclass
class SupportedTypesHandler(Protocol):
    protocol_id: ClassVar[int] = 25
    protocol_name: ClassVar[str] = "SupportedTypes"
    protocol_namespace: ClassVar[str] = "Energistics.Etp.v12.Protocol.SupportedTypes"

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

    # Define handlers for each message type in the protocol


    async def on_get_supported_types(
        self,
        msg: GetSupportedTypes,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetSupportedTypes messages.
        Message Type: 1
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_supported_types_response(
        self,
        msg: GetSupportedTypesResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetSupportedTypesResponse messages.
        Message Type: 2
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
