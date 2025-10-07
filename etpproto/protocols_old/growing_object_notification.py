# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncGenerator, Optional, Protocol, Union

from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader
from etptypes.energistics.etp.v12.protocol.growing_object_notification.part_subscription_ended import (
    PartSubscriptionEnded,
)
from etptypes.energistics.etp.v12.protocol.growing_object_notification.parts_changed import (
    PartsChanged,
)
from etptypes.energistics.etp.v12.protocol.growing_object_notification.parts_deleted import (
    PartsDeleted,
)
from etptypes.energistics.etp.v12.protocol.growing_object_notification.parts_replaced_by_range import (
    PartsReplacedByRange,
)
from etptypes.energistics.etp.v12.protocol.growing_object_notification.subscribe_part_notifications import (
    SubscribePartNotifications,
)
from etptypes.energistics.etp.v12.protocol.growing_object_notification.subscribe_part_notifications_response import (
    SubscribePartNotificationsResponse,
)
from etptypes.energistics.etp.v12.protocol.growing_object_notification.unsolicited_part_notifications import (
    UnsolicitedPartNotifications,
)
from etptypes.energistics.etp.v12.protocol.growing_object_notification.unsubscribe_part_notification import (
    UnsubscribePartNotification,
)

from etpproto.client_info import ClientInfo
from etpproto.error import InvalidMessageTypeError, NotSupportedError
from etpproto.messages import Message
from etpproto.utils import snake_case


class GrowingObjectNotificationHandler(Protocol):
    async def on_part_subscription_ended(
        self,
        msg: PartSubscriptionEnded,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_parts_changed(
        self,
        msg: PartsChanged,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_parts_deleted(
        self,
        msg: PartsDeleted,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_parts_replaced_by_range(
        self,
        msg: PartsReplacedByRange,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscribe_part_notifications(
        self,
        msg: SubscribePartNotifications,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscribe_part_notifications_response(
        self,
        msg: SubscribePartNotificationsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_unsolicited_part_notifications(
        self,
        msg: UnsolicitedPartNotifications,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_unsubscribe_part_notification(
        self,
        msg: UnsubscribePartNotification,
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
