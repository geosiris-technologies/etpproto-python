
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


from etptypes.energistics.etp.v12.protocol.data_array.put_data_arrays_response import PutDataArraysResponse
from etptypes.energistics.etp.v12.protocol.data_array.put_data_subarrays_response import PutDataSubarraysResponse
from etptypes.energistics.etp.v12.protocol.data_array.put_uninitialized_data_arrays_response import PutUninitializedDataArraysResponse
from etptypes.energistics.etp.v12.protocol.data_array.get_data_arrays_response import GetDataArraysResponse
from etptypes.energistics.etp.v12.protocol.data_array.get_data_subarrays_response import GetDataSubarraysResponse
from etptypes.energistics.etp.v12.protocol.data_array.get_data_array_metadata import GetDataArrayMetadata
from etptypes.energistics.etp.v12.protocol.data_array.get_data_arrays import GetDataArrays
from etptypes.energistics.etp.v12.protocol.data_array.get_data_subarrays import GetDataSubarrays
from etptypes.energistics.etp.v12.protocol.data_array.get_data_array_metadata_response import GetDataArrayMetadataResponse
from etptypes.energistics.etp.v12.protocol.data_array.put_data_arrays import PutDataArrays
from etptypes.energistics.etp.v12.protocol.data_array.put_data_subarrays import PutDataSubarrays
from etptypes.energistics.etp.v12.protocol.data_array.put_uninitialized_data_arrays import PutUninitializedDataArrays


@dataclass
class DataArrayHandler(Protocol):
    protocol_id: ClassVar[int] = 9
    protocol_name: ClassVar[str] = "DataArray"
    protocol_namespace: ClassVar[str] = "Energistics.Etp.v12.Protocol.DataArray"

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

    async def on_put_data_arrays_response(
        self,
        msg: PutDataArraysResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutDataArraysResponse messages.
        Message Type: 10
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_data_subarrays_response(
        self,
        msg: PutDataSubarraysResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutDataSubarraysResponse messages.
        Message Type: 11
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_uninitialized_data_arrays_response(
        self,
        msg: PutUninitializedDataArraysResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutUninitializedDataArraysResponse messages.
        Message Type: 12
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_arrays_response(
        self,
        msg: GetDataArraysResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDataArraysResponse messages.
        Message Type: 1
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_subarrays_response(
        self,
        msg: GetDataSubarraysResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDataSubarraysResponse messages.
        Message Type: 8
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_array_metadata(
        self,
        msg: GetDataArrayMetadata,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDataArrayMetadata messages.
        Message Type: 6
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_arrays(
        self,
        msg: GetDataArrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDataArrays messages.
        Message Type: 2
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_subarrays(
        self,
        msg: GetDataSubarrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDataSubarrays messages.
        Message Type: 3
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_array_metadata_response(
        self,
        msg: GetDataArrayMetadataResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetDataArrayMetadataResponse messages.
        Message Type: 7
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_data_arrays(
        self,
        msg: PutDataArrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutDataArrays messages.
        Message Type: 4
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_data_subarrays(
        self,
        msg: PutDataSubarrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutDataSubarrays messages.
        Message Type: 5
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_uninitialized_data_arrays(
        self,
        msg: PutUninitializedDataArrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutUninitializedDataArrays messages.
        Message Type: 9
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
