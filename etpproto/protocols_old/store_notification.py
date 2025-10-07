# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

from typing import AsyncGenerator, Optional, Protocol, Union

from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader
from etptypes.energistics.etp.v12.protocol.store_notification.chunk import (
    Chunk,
)
from etptypes.energistics.etp.v12.protocol.store_notification.object_access_revoked import (
    ObjectAccessRevoked,
)
from etptypes.energistics.etp.v12.protocol.store_notification.object_active_status_changed import (
    ObjectActiveStatusChanged,
)
from etptypes.energistics.etp.v12.protocol.store_notification.object_changed import (
    ObjectChanged,
)
from etptypes.energistics.etp.v12.protocol.store_notification.object_deleted import (
    ObjectDeleted,
)
from etptypes.energistics.etp.v12.protocol.store_notification.subscribe_notifications import (
    SubscribeNotifications,
)
from etptypes.energistics.etp.v12.protocol.store_notification.subscribe_notifications_response import (
    SubscribeNotificationsResponse,
)
from etptypes.energistics.etp.v12.protocol.store_notification.subscription_ended import (
    SubscriptionEnded,
)
from etptypes.energistics.etp.v12.protocol.store_notification.unsolicited_store_notifications import (
    UnsolicitedStoreNotifications,
)
from etptypes.energistics.etp.v12.protocol.store_notification.unsubscribe_notifications import (
    UnsubscribeNotifications,
)

from etpproto.client_info import ClientInfo
from etpproto.error import InvalidMessageTypeError, NotSupportedError
from etpproto.messages import Message
from etpproto.utils import snake_case


class StoreNotificationHandler(Protocol):
    async def on_chunk(
        self,
        msg: Chunk,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_object_access_revoked(
        self,
        msg: ObjectAccessRevoked,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_object_active_status_changed(
        self,
        msg: ObjectActiveStatusChanged,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_object_changed(
        self,
        msg: ObjectChanged,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_object_deleted(
        self,
        msg: ObjectDeleted,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscribe_notifications(
        self,
        msg: SubscribeNotifications,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscribe_notifications_response(
        self,
        msg: SubscribeNotificationsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_subscription_ended(
        self,
        msg: SubscriptionEnded,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_unsolicited_store_notifications(
        self,
        msg: UnsolicitedStoreNotifications,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )

    async def on_unsubscribe_notifications(
        self,
        msg: UnsubscribeNotifications,
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
