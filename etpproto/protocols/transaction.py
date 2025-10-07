
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

from etptypes.energistics.etp.v12.protocol.transaction.start_transaction import StartTransaction
from etptypes.energistics.etp.v12.protocol.transaction.commit_transaction import CommitTransaction
from etptypes.energistics.etp.v12.protocol.transaction.commit_transaction_response import CommitTransactionResponse
from etptypes.energistics.etp.v12.protocol.transaction.rollback_transaction import RollbackTransaction
from etptypes.energistics.etp.v12.protocol.transaction.rollback_transaction_response import RollbackTransactionResponse
from etptypes.energistics.etp.v12.protocol.transaction.start_transaction_response import StartTransactionResponse

@dataclass
class TransactionHandler(Protocol):
    protocol_id: ClassVar[int] = 18
    protocol_name: ClassVar[str] = "Transaction"
    protocol_namespace: ClassVar[str] = "Energistics.Etp.v12.Protocol.Transaction"

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


    async def on_start_transaction(
        self,
        msg: StartTransaction,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for StartTransaction messages.
        Message Type: 1
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_commit_transaction(
        self,
        msg: CommitTransaction,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for CommitTransaction messages.
        Message Type: 3
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_commit_transaction_response(
        self,
        msg: CommitTransactionResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for CommitTransactionResponse messages.
        Message Type: 5
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_rollback_transaction(
        self,
        msg: RollbackTransaction,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for RollbackTransaction messages.
        Message Type: 4
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_rollback_transaction_response(
        self,
        msg: RollbackTransactionResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for RollbackTransactionResponse messages.
        Message Type: 6
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_start_transaction_response(
        self,
        msg: StartTransactionResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for StartTransactionResponse messages.
        Message Type: 2
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
