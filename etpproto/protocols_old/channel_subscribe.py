# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncGenerator, Optional, Protocol, Union

from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader
from etptypes.energistics.etp.v12.protocol.channel_subscribe.cancel_get_ranges import (
    CancelGetRanges,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.channel_data import (
    ChannelData,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.channels_truncated import (
    ChannelsTruncated,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_change_annotations import (
    GetChangeAnnotations,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_change_annotations_response import (
    GetChangeAnnotationsResponse,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_channel_metadata import (
    GetChannelMetadata,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_channel_metadata_response import (
    GetChannelMetadataResponse,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_ranges import (
    GetRanges,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.get_ranges_response import (
    GetRangesResponse,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.range_replaced import (
    RangeReplaced,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.subscribe_channels import (
    SubscribeChannels,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.subscribe_channels_response import (
    SubscribeChannelsResponse,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.subscriptions_stopped import (
    SubscriptionsStopped,
)
from etptypes.energistics.etp.v12.protocol.channel_subscribe.unsubscribe_channels import (
    UnsubscribeChannels,
)

from etpproto.client_info import ClientInfo
from etpproto.error import InvalidMessageTypeError, NotSupportedError
from etpproto.messages import Message
from etpproto.utils import snake_case


class ChannelSubscribeHandler(Protocol):
    async def on_cancel_get_ranges(
        self,
        msg: CancelGetRanges,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_channel_data(
        self,
        msg: ChannelData,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_channels_truncated(
        self,
        msg: ChannelsTruncated,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_change_annotations(
        self,
        msg: GetChangeAnnotations,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_change_annotations_response(
        self,
        msg: GetChangeAnnotationsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_channel_metadata(
        self,
        msg: GetChannelMetadata,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_channel_metadata_response(
        self,
        msg: GetChannelMetadataResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_ranges(
        self,
        msg: GetRanges,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_ranges_response(
        self,
        msg: GetRangesResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_range_replaced(
        self,
        msg: RangeReplaced,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscribe_channels(
        self,
        msg: SubscribeChannels,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscribe_channels_response(
        self,
        msg: SubscribeChannelsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscriptions_stopped(
        self,
        msg: SubscriptionsStopped,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_unsubscribe_channels(
        self,
        msg: UnsubscribeChannels,
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
