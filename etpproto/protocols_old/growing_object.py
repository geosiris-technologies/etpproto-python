# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncGenerator, Optional, Protocol, Union

from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader
from etptypes.energistics.etp.v12.protocol.growing_object.delete_parts import (
    DeleteParts,
)
from etptypes.energistics.etp.v12.protocol.growing_object.delete_parts_response import (
    DeletePartsResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_change_annotations import (
    GetChangeAnnotations,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_change_annotations_response import (
    GetChangeAnnotationsResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_growing_data_objects_header import (
    GetGrowingDataObjectsHeader,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_growing_data_objects_header_response import (
    GetGrowingDataObjectsHeaderResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts import (
    GetParts,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_by_range import (
    GetPartsByRange,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_by_range_response import (
    GetPartsByRangeResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_metadata import (
    GetPartsMetadata,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_metadata_response import (
    GetPartsMetadataResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.get_parts_response import (
    GetPartsResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.put_growing_data_objects_header import (
    PutGrowingDataObjectsHeader,
)
from etptypes.energistics.etp.v12.protocol.growing_object.put_growing_data_objects_header_response import (
    PutGrowingDataObjectsHeaderResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.put_parts import (
    PutParts,
)
from etptypes.energistics.etp.v12.protocol.growing_object.put_parts_response import (
    PutPartsResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object.replace_parts_by_range import (
    ReplacePartsByRange,
)
from etptypes.energistics.etp.v12.protocol.growing_object.replace_parts_by_range_response import (
    ReplacePartsByRangeResponse,
)

from etpproto.client_info import ClientInfo
from etpproto.error import InvalidMessageTypeError, NotSupportedError
from etpproto.messages import Message
from etpproto.utils import snake_case


class GrowingObjectHandler(Protocol):
    async def on_delete_parts(
        self,
        msg: DeleteParts,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_delete_parts_response(
        self,
        msg: DeletePartsResponse,
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

    async def on_get_growing_data_objects_header(
        self,
        msg: GetGrowingDataObjectsHeader,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_growing_data_objects_header_response(
        self,
        msg: GetGrowingDataObjectsHeaderResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts(
        self,
        msg: GetParts,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts_by_range(
        self,
        msg: GetPartsByRange,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts_by_range_response(
        self,
        msg: GetPartsByRangeResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts_metadata(
        self,
        msg: GetPartsMetadata,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts_metadata_response(
        self,
        msg: GetPartsMetadataResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_get_parts_response(
        self,
        msg: GetPartsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_growing_data_objects_header(
        self,
        msg: PutGrowingDataObjectsHeader,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_growing_data_objects_header_response(
        self,
        msg: PutGrowingDataObjectsHeaderResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_parts(
        self,
        msg: PutParts,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_put_parts_response(
        self,
        msg: PutPartsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_replace_parts_by_range(
        self,
        msg: ReplacePartsByRange,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_replace_parts_by_range_response(
        self,
        msg: ReplacePartsByRangeResponse,
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
