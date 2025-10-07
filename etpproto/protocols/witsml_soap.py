
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


from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_add_to_store import WMLS_AddToStore
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_add_to_store_response import WMLS_AddToStoreResponse
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_delete_from_store import WMLS_DeleteFromStore
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_delete_from_store_response import WMLS_DeleteFromStoreResponse
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_get_base_msg import WMLS_GetBaseMsg
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_get_cap import WMLS_GetCap
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_get_base_msg_response import WMLS_GetBaseMsgResponse
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_get_from_store import WMLS_GetFromStore
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_get_cap_response import WMLS_GetCapResponse
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_get_from_store_response import WMLS_GetFromStoreResponse
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_get_version import WMLS_GetVersion
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_get_version_response import WMLS_GetVersionResponse
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_update_in_store import WMLS_UpdateInStore
from etptypes.energistics.etp.v12.private_protocols.witsml_soap.wmls_update_in_store_response import WMLS_UpdateInStoreResponse


@dataclass
class WitsmlSoapHandler(Protocol):
    protocol_id: ClassVar[int] = 2100
    protocol_name: ClassVar[str] = "WitsmlSoap"
    protocol_namespace: ClassVar[str] = "Energistics.Etp.v12.PrivateProtocols.WitsmlSoap"

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

    async def on_wmls_add_to_store(
        self,
        msg: WMLS_AddToStore,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_AddToStore messages.
        Message Type: 1
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_add_to_store_response(
        self,
        msg: WMLS_AddToStoreResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_AddToStoreResponse messages.
        Message Type: 2
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_delete_from_store(
        self,
        msg: WMLS_DeleteFromStore,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_DeleteFromStore messages.
        Message Type: 3
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_delete_from_store_response(
        self,
        msg: WMLS_DeleteFromStoreResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_DeleteFromStoreResponse messages.
        Message Type: 4
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_get_base_msg(
        self,
        msg: WMLS_GetBaseMsg,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_GetBaseMsg messages.
        Message Type: 5
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_get_cap(
        self,
        msg: WMLS_GetCap,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_GetCap messages.
        Message Type: 7
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_get_base_msg_response(
        self,
        msg: WMLS_GetBaseMsgResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_GetBaseMsgResponse messages.
        Message Type: 6
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_get_from_store(
        self,
        msg: WMLS_GetFromStore,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_GetFromStore messages.
        Message Type: 9
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_get_cap_response(
        self,
        msg: WMLS_GetCapResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_GetCapResponse messages.
        Message Type: 8
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_get_from_store_response(
        self,
        msg: WMLS_GetFromStoreResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_GetFromStoreResponse messages.
        Message Type: 10
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_get_version(
        self,
        msg: WMLS_GetVersion,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_GetVersion messages.
        Message Type: 11
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_get_version_response(
        self,
        msg: WMLS_GetVersionResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_GetVersionResponse messages.
        Message Type: 12
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_update_in_store(
        self,
        msg: WMLS_UpdateInStore,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_UpdateInStore messages.
        Message Type: 13
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_wmls_update_in_store_response(
        self,
        msg: WMLS_UpdateInStoreResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for WMLS_UpdateInStoreResponse messages.
        Message Type: 14
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
