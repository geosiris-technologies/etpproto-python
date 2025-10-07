
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

from etptypes.energistics.etp.v12.protocol.growing_object.delete_parts import DeleteParts
from etptypes.energistics.etp.v12.protocol.growing_object.delete_parts_response import DeletePartsResponse
from etptypes.energistics.etp.v12.protocol.growing_object.get_change_annotations import GetChangeAnnotations
from etptypes.energistics.etp.v12.protocol.growing_object.get_growing_data_objects_header import GetGrowingDataObjectsHeader
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts import GetParts
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_metadata import GetPartsMetadata
from etptypes.energistics.etp.v12.protocol.growing_object.put_growing_data_objects_header_response import PutGrowingDataObjectsHeaderResponse
from etptypes.energistics.etp.v12.protocol.growing_object.put_parts_response import PutPartsResponse
from etptypes.energistics.etp.v12.protocol.growing_object.replace_parts_by_range_response import ReplacePartsByRangeResponse
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_by_range import GetPartsByRange
from etptypes.energistics.etp.v12.protocol.growing_object.get_change_annotations_response import GetChangeAnnotationsResponse
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_by_range_response import GetPartsByRangeResponse
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_response import GetPartsResponse
from etptypes.energistics.etp.v12.protocol.growing_object.put_parts import PutParts
from etptypes.energistics.etp.v12.protocol.growing_object.replace_parts_by_range import ReplacePartsByRange
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_metadata_response import GetPartsMetadataResponse
from etptypes.energistics.etp.v12.protocol.growing_object.get_growing_data_objects_header_response import GetGrowingDataObjectsHeaderResponse
from etptypes.energistics.etp.v12.protocol.growing_object.put_growing_data_objects_header import PutGrowingDataObjectsHeader

@dataclass
class GrowingObjectHandler(Protocol):
    protocol_id: ClassVar[int] = 6
    protocol_name: ClassVar[str] = "GrowingObject"
    protocol_namespace: ClassVar[str] = "Energistics.Etp.v12.Protocol.GrowingObject"

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


    async def on_delete_parts(
        self,
        msg: DeleteParts,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for DeleteParts messages.
        Message Type: 1
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_delete_parts_response(
        self,
        msg: DeletePartsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for DeletePartsResponse messages.
        Message Type: 11
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_change_annotations(
        self,
        msg: GetChangeAnnotations,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetChangeAnnotations messages.
        Message Type: 19
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_growing_data_objects_header(
        self,
        msg: GetGrowingDataObjectsHeader,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetGrowingDataObjectsHeader messages.
        Message Type: 14
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts(
        self,
        msg: GetParts,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetParts messages.
        Message Type: 3
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts_metadata(
        self,
        msg: GetPartsMetadata,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetPartsMetadata messages.
        Message Type: 8
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_growing_data_objects_header_response(
        self,
        msg: PutGrowingDataObjectsHeaderResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutGrowingDataObjectsHeaderResponse messages.
        Message Type: 17
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_parts_response(
        self,
        msg: PutPartsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutPartsResponse messages.
        Message Type: 13
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_replace_parts_by_range_response(
        self,
        msg: ReplacePartsByRangeResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for ReplacePartsByRangeResponse messages.
        Message Type: 18
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts_by_range(
        self,
        msg: GetPartsByRange,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetPartsByRange messages.
        Message Type: 4
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_change_annotations_response(
        self,
        msg: GetChangeAnnotationsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetChangeAnnotationsResponse messages.
        Message Type: 20
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts_by_range_response(
        self,
        msg: GetPartsByRangeResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetPartsByRangeResponse messages.
        Message Type: 10
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts_response(
        self,
        msg: GetPartsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetPartsResponse messages.
        Message Type: 6
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_parts(
        self,
        msg: PutParts,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutParts messages.
        Message Type: 5
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_replace_parts_by_range(
        self,
        msg: ReplacePartsByRange,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for ReplacePartsByRange messages.
        Message Type: 7
        Sender Role: customer
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts_metadata_response(
        self,
        msg: GetPartsMetadataResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetPartsMetadataResponse messages.
        Message Type: 9
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_growing_data_objects_header_response(
        self,
        msg: GetGrowingDataObjectsHeaderResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetGrowingDataObjectsHeaderResponse messages.
        Message Type: 15
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_growing_data_objects_header(
        self,
        msg: PutGrowingDataObjectsHeader,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PutGrowingDataObjectsHeader messages.
        Message Type: 16
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
