
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


from etptypes.energistics.etp.v12.protocol.discovery.get_deleted_resources import GetDeletedResources
from etptypes.energistics.etp.v12.protocol.discovery.get_deleted_resources_response import GetDeletedResourcesResponse
from etptypes.energistics.etp.v12.protocol.discovery.get_resources import GetResources
from etptypes.energistics.etp.v12.protocol.discovery.get_resources_edges_response import GetResourcesEdgesResponse
from etptypes.energistics.etp.v12.protocol.discovery.get_resources_response import GetResourcesResponse


@dataclass
class DiscoveryHandler(Protocol):
    protocol_id: ClassVar[int] = 3
    protocol_name: ClassVar[str] = "Discovery"
    protocol_namespace: ClassVar[str] = "Energistics.Etp.v12.Protocol.Discovery"

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

    async def on_get_deleted_resources(
        self,
        msg: GetDeletedResources,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDeletedResources messages.
        Message Type: 5
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_deleted_resources_response(
        self,
        msg: GetDeletedResourcesResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDeletedResourcesResponse messages.
        Message Type: 6
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_resources(
        self,
        msg: GetResources,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetResources messages.
        Message Type: 1
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_resources_edges_response(
        self,
        msg: GetResourcesEdgesResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetResourcesEdgesResponse messages.
        Message Type: 7
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_resources_response(
        self,
        msg: GetResourcesResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetResourcesResponse messages.
        Message Type: 4
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
