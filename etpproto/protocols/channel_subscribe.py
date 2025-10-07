
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


from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_channel_metadata import GetChannelMetadata
from etptypes.energistics.etp.v12.protocol.channel_subscribe.subscribe_channels_response import SubscribeChannelsResponse
from etptypes.energistics.etp.v12.protocol.channel_subscribe.subscriptions_stopped import SubscriptionsStopped
from etptypes.energistics.etp.v12.protocol.channel_subscribe.unsubscribe_channels import UnsubscribeChannels
from etptypes.energistics.etp.v12.protocol.channel_subscribe.cancel_get_ranges import CancelGetRanges
from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_change_annotations import GetChangeAnnotations
from etptypes.energistics.etp.v12.protocol.channel_subscribe.subscribe_channels import SubscribeChannels
from etptypes.energistics.etp.v12.protocol.channel_subscribe.channel_data import ChannelData
from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_ranges_response import GetRangesResponse
from etptypes.energistics.etp.v12.protocol.channel_subscribe.channels_truncated import ChannelsTruncated
from etptypes.energistics.etp.v12.protocol.channel_subscribe.range_replaced import RangeReplaced
from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_ranges import GetRanges
from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_channel_metadata_response import GetChannelMetadataResponse
from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_change_annotations_response import GetChangeAnnotationsResponse


@dataclass
class ChannelSubscribeHandler(Protocol):
    protocol_id: ClassVar[int] = 21
    protocol_name: ClassVar[str] = "ChannelSubscribe"
    protocol_namespace: ClassVar[str] = "Energistics.Etp.v12.Protocol.ChannelSubscribe"

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

    async def on_get_channel_metadata(
        self,
        msg: GetChannelMetadata,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetChannelMetadata messages.
        Message Type: 1
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscribe_channels_response(
        self,
        msg: SubscribeChannelsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for SubscribeChannelsResponse messages.
        Message Type: 12
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscriptions_stopped(
        self,
        msg: SubscriptionsStopped,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for SubscriptionsStopped messages.
        Message Type: 8
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_unsubscribe_channels(
        self,
        msg: UnsubscribeChannels,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for UnsubscribeChannels messages.
        Message Type: 7
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_cancel_get_ranges(
        self,
        msg: CancelGetRanges,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for CancelGetRanges messages.
        Message Type: 11
        Sender Role: customer
        Multipart: False
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
        Message Type: 14
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscribe_channels(
        self,
        msg: SubscribeChannels,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for SubscribeChannels messages.
        Message Type: 3
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_channel_data(
        self,
        msg: ChannelData,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for ChannelData messages.
        Message Type: 4
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_ranges_response(
        self,
        msg: GetRangesResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetRangesResponse messages.
        Message Type: 10
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_channels_truncated(
        self,
        msg: ChannelsTruncated,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for ChannelsTruncated messages.
        Message Type: 13
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_range_replaced(
        self,
        msg: RangeReplaced,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for RangeReplaced messages.
        Message Type: 6
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_ranges(
        self,
        msg: GetRanges,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetRanges messages.
        Message Type: 9
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_channel_metadata_response(
        self,
        msg: GetChannelMetadataResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for GetChannelMetadataResponse messages.
        Message Type: 2
        Sender Role: store
        Multipart: True
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
        Message Type: 15
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
