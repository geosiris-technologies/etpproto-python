
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

from etptypes.energistics.etp.v12.protocol.growing_object_notification.subscribe_part_notifications_response import SubscribePartNotificationsResponse
from etptypes.energistics.etp.v12.protocol.growing_object_notification.parts_deleted import PartsDeleted
from etptypes.energistics.etp.v12.protocol.growing_object_notification.part_subscription_ended import PartSubscriptionEnded
from etptypes.energistics.etp.v12.protocol.growing_object_notification.unsubscribe_part_notification import UnsubscribePartNotification
from etptypes.energistics.etp.v12.protocol.growing_object_notification.parts_changed import PartsChanged
from etptypes.energistics.etp.v12.protocol.growing_object_notification.parts_replaced_by_range import PartsReplacedByRange
from etptypes.energistics.etp.v12.protocol.growing_object_notification.subscribe_part_notifications import SubscribePartNotifications
from etptypes.energistics.etp.v12.protocol.growing_object_notification.unsolicited_part_notifications import UnsolicitedPartNotifications

@dataclass
class GrowingObjectNotificationHandler(Protocol):
    protocol_id: ClassVar[int] = 7
    protocol_name: ClassVar[str] = "GrowingObjectNotification"
    protocol_namespace: ClassVar[str] = "Energistics.Etp.v12.Protocol.GrowingObjectNotification"

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


    async def on_subscribe_part_notifications_response(
        self,
        msg: SubscribePartNotificationsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for SubscribePartNotificationsResponse messages.
        Message Type: 10
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_parts_deleted(
        self,
        msg: PartsDeleted,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PartsDeleted messages.
        Message Type: 3
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_part_subscription_ended(
        self,
        msg: PartSubscriptionEnded,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PartSubscriptionEnded messages.
        Message Type: 8
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_unsubscribe_part_notification(
        self,
        msg: UnsubscribePartNotification,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for UnsubscribePartNotification messages.
        Message Type: 4
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_parts_changed(
        self,
        msg: PartsChanged,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PartsChanged messages.
        Message Type: 2
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_parts_replaced_by_range(
        self,
        msg: PartsReplacedByRange,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for PartsReplacedByRange messages.
        Message Type: 6
        Sender Role: store
        Multipart: True
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscribe_part_notifications(
        self,
        msg: SubscribePartNotifications,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for SubscribePartNotifications messages.
        Message Type: 7
        Sender Role: customer
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_unsolicited_part_notifications(
        self,
        msg: UnsolicitedPartNotifications,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        '''
        Handler for UnsolicitedPartNotifications messages.
        Message Type: 9
        Sender Role: store
        Multipart: False
        '''
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
