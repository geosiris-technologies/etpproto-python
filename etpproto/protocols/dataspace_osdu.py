
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


from etptypes.energistics.etp.v12.protocol.dataspace_osdu.get_dataspace_info import GetDataspaceInfo
from etptypes.energistics.etp.v12.protocol.dataspace_osdu.get_dataspace_info_response import GetDataspaceInfoResponse
from etptypes.energistics.etp.v12.protocol.dataspace_osdu.copy_dataspaces_content import CopyDataspacesContent
from etptypes.energistics.etp.v12.protocol.dataspace_osdu.copy_dataspaces_content_response import CopyDataspacesContentResponse
from etptypes.energistics.etp.v12.protocol.dataspace_osdu.lock_dataspaces import LockDataspaces
from etptypes.energistics.etp.v12.protocol.dataspace_osdu.lock_dataspaces_response import LockDataspacesResponse
from etptypes.energistics.etp.v12.protocol.dataspace_osdu.copy_to_dataspace import CopyToDataspace
from etptypes.energistics.etp.v12.protocol.dataspace_osdu.copy_to_dataspace_response import CopyToDataspaceResponse


@dataclass
class DataspaceOSDUHandler(Protocol):
    protocol_id: ClassVar[int] = 2424
    protocol_name: ClassVar[str] = "DataspaceOSDU"
    protocol_namespace: ClassVar[str] = "Energistics.Etp.v12.Protocol.DataspaceOSDU"

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

    async def on_get_dataspace_info(
        self,
        msg: GetDataspaceInfo,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDataspaceInfo messages.
        Message Type: 1
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_dataspace_info_response(
        self,
        msg: GetDataspaceInfoResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDataspaceInfoResponse messages.
        Message Type: 2
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_copy_dataspaces_content(
        self,
        msg: CopyDataspacesContent,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for CopyDataspacesContent messages.
        Message Type: 3
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_copy_dataspaces_content_response(
        self,
        msg: CopyDataspacesContentResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for CopyDataspacesContentResponse messages.
        Message Type: 4
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_lock_dataspaces(
        self,
        msg: LockDataspaces,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for LockDataspaces messages.
        Message Type: 5
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_lock_dataspaces_response(
        self,
        msg: LockDataspacesResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for LockDataspacesResponse messages.
        Message Type: 6
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_copy_to_dataspace(
        self,
        msg: CopyToDataspace,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for CopyToDataspace messages.
        Message Type: 7
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_copy_to_dataspace_response(
        self,
        msg: CopyToDataspaceResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for CopyToDataspaceResponse messages.
        Message Type: 8
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
