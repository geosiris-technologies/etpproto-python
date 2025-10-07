# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncGenerator, Optional, Protocol, Union

from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader
from etptypes.energistics.etp.v12.protocol.channel_data_load.channel_data import (
    ChannelData,
)
from etptypes.energistics.etp.v12.protocol.channel_data_load.channels_closed import (
    ChannelsClosed,
)
from etptypes.energistics.etp.v12.protocol.channel_data_load.close_channels import (
    CloseChannels,
)
from etptypes.energistics.etp.v12.protocol.channel_data_load.open_channels import (
    OpenChannels,
)
from etptypes.energistics.etp.v12.protocol.channel_data_load.open_channels_response import (
    OpenChannelsResponse,
)
from etptypes.energistics.etp.v12.protocol.channel_data_load.replace_range import (
    ReplaceRange,
)
from etptypes.energistics.etp.v12.protocol.channel_data_load.replace_range_response import (
    ReplaceRangeResponse,
)
from etptypes.energistics.etp.v12.protocol.channel_data_load.truncate_channels import (
    TruncateChannels,
)
from etptypes.energistics.etp.v12.protocol.channel_data_load.truncate_channels_response import (
    TruncateChannelsResponse,
)

from etpproto.client_info import ClientInfo
from etpproto.error import InvalidMessageTypeError, NotSupportedError
from etpproto.messages import Message
from etpproto.utils import snake_case


class ChannelDataLoadHandler(Protocol):
    async def on_channel_data(
        self,
        msg: ChannelData,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_channels_closed(
        self,
        msg: ChannelsClosed,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_close_channels(
        self,
        msg: CloseChannels,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_open_channels(
        self,
        msg: OpenChannels,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_open_channels_response(
        self,
        msg: OpenChannelsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_replace_range(
        self,
        msg: ReplaceRange,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_replace_range_response(
        self,
        msg: ReplaceRangeResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_truncate_channels(
        self,
        msg: TruncateChannels,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_truncate_channels_response(
        self,
        msg: TruncateChannelsResponse,
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
