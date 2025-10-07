from typing import Protocol, Union, AsyncGenerator, Optional
from etpproto.error import NotSupportedError, InvalidMessageTypeError
from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader
from etpproto.messages import Message
from etpproto.utils import snake_case
from etpproto.client_info import ClientInfo
from etptypes.energistics.etp.v12.protocol.store_osdu.copy_data_objects_by_value import (
    CopyDataObjectsByValue,
)
from etptypes.energistics.etp.v12.protocol.store_osdu.copy_data_objects_by_value_response import (
    CopyDataObjectsByValueResponse,
)


class StoreOsduHandler(Protocol):

    async def on_copy_data_objects_by_value(
        self,
        msg: CopyDataObjectsByValue,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_copy_data_objects_by_value_response(
        self,
        msg: CopyDataObjectsByValueResponse,
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
