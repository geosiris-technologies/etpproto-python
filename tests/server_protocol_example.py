import uuid
from datetime import datetime
from typing import AsyncGenerator, Optional, Union

from etptypes.energistics.etp.v12.datatypes.contact import Contact
from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader
from etptypes.energistics.etp.v12.datatypes.server_capabilities import (
    ServerCapabilities,
)
from etptypes.energistics.etp.v12.datatypes.supported_data_object import (
    SupportedDataObject,
)
from etptypes.energistics.etp.v12.datatypes.supported_protocol import (
    SupportedProtocol,
)
from etptypes.energistics.etp.v12.datatypes.uuid import Uuid
from etptypes.energistics.etp.v12.datatypes.version import Version
from etptypes.energistics.etp.v12.protocol.core.close_session import (
    CloseSession,
)
from etptypes.energistics.etp.v12.protocol.core.open_session import OpenSession
from etptypes.energistics.etp.v12.protocol.core.ping import Ping
from etptypes.energistics.etp.v12.protocol.core.pong import Pong
from etptypes.energistics.etp.v12.protocol.core.request_session import (
    RequestSession,
)
from etptypes.energistics.etp.v12.protocol.store.get_data_objects_response import (
    GetDataObjectsResponse,
)

from etpproto.client_info import ClientInfo
from etpproto.connection import (
    CommunicationProtocol,
    ConnectionType,
    ETPConnection,
)
from etpproto.error import NotSupportedError
from etpproto.messages import Message
from etpproto.protocols.core import CoreHandler
from etpproto.protocols.store import StoreHandler

#    ______                                    __                   __
#   / ____/___  ________     ____  _________  / /_____  _________  / /
#  / /   / __ \/ ___/ _ \   / __ \/ ___/ __ \/ __/ __ \/ ___/ __ \/ /
# / /___/ /_/ / /  /  __/  / /_/ / /  / /_/ / /_/ /_/ / /__/ /_/ / /
# \____/\____/_/   \___/  / .___/_/   \____/\__/\____/\___/\____/_/
#                        /_/


@ETPConnection.on(CommunicationProtocol.CORE)
class myCoreProtocol(CoreHandler):
    async def on_request_session(
        self,
        msg: RequestSession,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo],
    ) -> AsyncGenerator[Optional[Message], None]:
        print("RequestSession recieved, answer with OpenSession")
        supportedProtocolList = ETPConnection.get_supported_protocol_list()
        openSession = OpenSession(
            application_name="etpproto",
            application_version="1.0",
            server_instance_id=uuid.uuid4(),
            supported_protocols=supportedProtocolList,
            supported_data_objects=[
                SupportedDataObject(
                    qualified_type="resqml20",
                    data_object_capabilities={},
                )
            ],
            supported_compression="string",
            supported_formats=["xml"],
            session_id=msg.client_instance_id,
            current_date_time=int(datetime.utcnow().timestamp()),
            endpoint_capabilities={},
            earliest_retained_change_time=int(datetime.utcnow().timestamp()),
        )
        # TODO: Attention ici le msgId est mauvais il faudra le changer a posteriori
        yield Message.get_object_message(
            openSession, correlation_id=msg_header.message_id
        )

    async def on_close_session(
        self,
        msg: CloseSession,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo],
    ) -> AsyncGenerator[Optional[Message], None]:
        print("closing")

    async def on_ping(
        self,
        msg: Ping,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo],
    ) -> AsyncGenerator[Optional[Message], None]:
        print("#Core : Ping recieved")
        yield Message.get_object_message(
            Pong(current_date_time=int(datetime.utcnow().timestamp())),
            correlation_id=msg_header.message_id,
        )

    async def on_pong(
        self,
        msg: Pong,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo],
    ) -> AsyncGenerator[Optional[Message], None]:
        print("#Core : Pong recieved")


@ETPConnection.on(CommunicationProtocol.STORE)
class myStoreProtocol(StoreHandler):
    async def on_get_data_objects_response(
        self,
        msg: GetDataObjectsResponse,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        print("GetDataObjectsResponse recieved")
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )
