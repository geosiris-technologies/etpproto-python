# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncGenerator, Optional, Protocol, Union

from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader
from etptypes.energistics.etp.v12.protocol.channel_data_frame.cancel_get_frame import (
    CancelGetFrame,
)
from etptypes.energistics.etp.v12.protocol.channel_data_frame.get_frame import (
    GetFrame,
)
from etptypes.energistics.etp.v12.protocol.channel_data_frame.get_frame_metadata import (
    GetFrameMetadata,
)
from etptypes.energistics.etp.v12.protocol.channel_data_frame.get_frame_metadata_response import (
    GetFrameMetadataResponse,
)
from etptypes.energistics.etp.v12.protocol.channel_data_frame.get_frame_response_header import (
    GetFrameResponseHeader,
)
from etptypes.energistics.etp.v12.protocol.channel_data_frame.get_frame_response_rows import (
    GetFrameResponseRows,
)

from etpproto.client_info import ClientInfo
from etpproto.error import InvalidMessageTypeError, NotSupportedError
from etpproto.messages import Message
from etpproto.utils import snake_case


class ChannelDataFrameHandler(Protocol):
    async def on_cancel_get_frame(
        self,
        msg: CancelGetFrame,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_frame(
        self,
        msg: GetFrame,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_frame_metadata(
        self,
        msg: GetFrameMetadata,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_frame_metadata_response(
        self,
        msg: GetFrameMetadataResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_frame_response_header(
        self,
        msg: GetFrameResponseHeader,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_frame_response_rows(
        self,
        msg: GetFrameResponseRows,
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
