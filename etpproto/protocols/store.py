
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

from etptypes.energistics.etp.v12.protocol.store.delete_data_objects import DeleteDataObjects
from etptypes.energistics.etp.v12.protocol.store.get_data_objects import GetDataObjects
from etptypes.energistics.etp.v12.protocol.store.delete_data_objects_response import DeleteDataObjectsResponse
from etptypes.energistics.etp.v12.protocol.store.chunk import Chunk
from etptypes.energistics.etp.v12.protocol.store.put_data_objects_response import PutDataObjectsResponse
from etptypes.energistics.etp.v12.protocol.store.get_data_objects_response import GetDataObjectsResponse
from etptypes.energistics.etp.v12.protocol.store.put_data_objects import PutDataObjects

@dataclass
class StoreHandler(Protocol):
    protocol_id: ClassVar[int] = 4
    protocol_name: ClassVar[str] = "Store"
    protocol_namespace: ClassVar[str] = "Energistics.Etp.v12.Protocol.Store"

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


    async def on_delete_data_objects(
        self,
        msg: DeleteDataObjects,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for DeleteDataObjects messages.
        Message Type: 3
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_objects(
        self,
        msg: GetDataObjects,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDataObjects messages.
        Message Type: 1
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_delete_data_objects_response(
        self,
        msg: DeleteDataObjectsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for DeleteDataObjectsResponse messages.
        Message Type: 10
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_chunk(
        self,
        msg: Chunk,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for Chunk messages.
        Message Type: 8
        Sender Role: store,customer
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_data_objects_response(
        self,
        msg: PutDataObjectsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutDataObjectsResponse messages.
        Message Type: 9
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_objects_response(
        self,
        msg: GetDataObjectsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDataObjectsResponse messages.
        Message Type: 4
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_data_objects(
        self,
        msg: PutDataObjects,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutDataObjects messages.
        Message Type: 2
        Sender Role: customer
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
