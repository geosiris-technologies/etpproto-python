
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

from etptypes.energistics.etp.v12.protocol.store_notification.subscribe_notifications_response import SubscribeNotificationsResponse
from etptypes.energistics.etp.v12.protocol.store_notification.chunk import Chunk
from etptypes.energistics.etp.v12.protocol.store_notification.object_access_revoked import ObjectAccessRevoked
from etptypes.energistics.etp.v12.protocol.store_notification.object_deleted import ObjectDeleted
from etptypes.energistics.etp.v12.protocol.store_notification.subscription_ended import SubscriptionEnded
from etptypes.energistics.etp.v12.protocol.store_notification.unsubscribe_notifications import UnsubscribeNotifications
from etptypes.energistics.etp.v12.protocol.store_notification.object_active_status_changed import ObjectActiveStatusChanged
from etptypes.energistics.etp.v12.protocol.store_notification.object_changed import ObjectChanged
from etptypes.energistics.etp.v12.protocol.store_notification.subscribe_notifications import SubscribeNotifications
from etptypes.energistics.etp.v12.protocol.store_notification.unsolicited_store_notifications import UnsolicitedStoreNotifications

@dataclass
class StoreNotificationHandler(Protocol):
    protocol_id: ClassVar[int] = 5
    protocol_name: ClassVar[str] = "StoreNotification"
    protocol_namespace: ClassVar[str] = "Energistics.Etp.v12.Protocol.StoreNotification"

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


    async def on_subscribe_notifications_response(
        self,
        msg: SubscribeNotificationsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for SubscribeNotificationsResponse messages.
        Message Type: 10
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_chunk(
        self,
        msg: Chunk,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for Chunk messages.
        Message Type: 9
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_object_access_revoked(
        self,
        msg: ObjectAccessRevoked,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for ObjectAccessRevoked messages.
        Message Type: 5
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_object_deleted(
        self,
        msg: ObjectDeleted,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for ObjectDeleted messages.
        Message Type: 3
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscription_ended(
        self,
        msg: SubscriptionEnded,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for SubscriptionEnded messages.
        Message Type: 7
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_unsubscribe_notifications(
        self,
        msg: UnsubscribeNotifications,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for UnsubscribeNotifications messages.
        Message Type: 4
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_object_active_status_changed(
        self,
        msg: ObjectActiveStatusChanged,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for ObjectActiveStatusChanged messages.
        Message Type: 11
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_object_changed(
        self,
        msg: ObjectChanged,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for ObjectChanged messages.
        Message Type: 2
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscribe_notifications(
        self,
        msg: SubscribeNotifications,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for SubscribeNotifications messages.
        Message Type: 6
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_unsolicited_store_notifications(
        self,
        msg: UnsolicitedStoreNotifications,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for UnsolicitedStoreNotifications messages.
        Message Type: 8
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
