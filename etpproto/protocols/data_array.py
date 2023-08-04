# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncGenerator, Optional, Protocol, Union

from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader
from etptypes.energistics.etp.v12.protocol.data_array.get_data_array_metadata import (
    GetDataArrayMetadata,
)
from etptypes.energistics.etp.v12.protocol.data_array.get_data_array_metadata_response import (
    GetDataArrayMetadataResponse,
)
from etptypes.energistics.etp.v12.protocol.data_array.get_data_arrays import (
    GetDataArrays,
)
from etptypes.energistics.etp.v12.protocol.data_array.get_data_arrays_response import (
    GetDataArraysResponse,
)
from etptypes.energistics.etp.v12.protocol.data_array.get_data_subarrays import (
    GetDataSubarrays,
)
from etptypes.energistics.etp.v12.protocol.data_array.get_data_subarrays_response import (
    GetDataSubarraysResponse,
)
from etptypes.energistics.etp.v12.protocol.data_array.put_data_arrays import (
    PutDataArrays,
)
from etptypes.energistics.etp.v12.protocol.data_array.put_data_arrays_response import (
    PutDataArraysResponse,
)
from etptypes.energistics.etp.v12.protocol.data_array.put_data_subarrays import (
    PutDataSubarrays,
)
from etptypes.energistics.etp.v12.protocol.data_array.put_data_subarrays_response import (
    PutDataSubarraysResponse,
)
from etptypes.energistics.etp.v12.protocol.data_array.put_uninitialized_data_arrays import (
    PutUninitializedDataArrays,
)
from etptypes.energistics.etp.v12.protocol.data_array.put_uninitialized_data_arrays_response import (
    PutUninitializedDataArraysResponse,
)

from etpproto.client_info import ClientInfo
from etpproto.error import InvalidMessageTypeError, NotSupportedError
from etpproto.messages import Message
from etpproto.utils import snake_case


class DataArrayHandler(Protocol):
    async def on_get_data_array_metadata(
        self,
        msg: GetDataArrayMetadata,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_array_metadata_response(
        self,
        msg: GetDataArrayMetadataResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_arrays(
        self,
        msg: GetDataArrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_arrays_response(
        self,
        msg: GetDataArraysResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_subarrays(
        self,
        msg: GetDataSubarrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_data_subarrays_response(
        self,
        msg: GetDataSubarraysResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_data_arrays(
        self,
        msg: PutDataArrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_data_arrays_response(
        self,
        msg: PutDataArraysResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_data_subarrays(
        self,
        msg: PutDataSubarrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_data_subarrays_response(
        self,
        msg: PutDataSubarraysResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_uninitialized_data_arrays(
        self,
        msg: PutUninitializedDataArrays,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_uninitialized_data_arrays_response(
        self,
        msg: PutUninitializedDataArraysResponse,
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
