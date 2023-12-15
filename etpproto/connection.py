# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging

from dataclasses import dataclass, field
from enum import Enum
from typing import (
    AsyncGenerator,
    Callable,
    ClassVar,
    Final,
    List,
    Dict,
    Optional,
    Tuple,
    Type,
    Union,
)

from etptypes.energistics.etp.v12.datatypes.server_capabilities import (
    ServerCapabilities,
)
from etptypes.energistics.etp.v12.datatypes.supported_protocol import (
    SupportedProtocol,
)
from etptypes.energistics.etp.v12.protocol.core.acknowledge import Acknowledge
from etptypes.energistics.etp.v12.protocol.core.close_session import (
    CloseSession,
)
from etptypes.energistics.etp.v12.protocol.core.authorize_response import (
    AuthorizeResponse,
)
from etptypes.energistics.etp.v12.protocol.core.authorize import Authorize
from etptypes.energistics.etp.v12.protocol.core.open_session import OpenSession
from etptypes.energistics.etp.v12.protocol.core.request_session import (
    RequestSession,
)
from etptypes.energistics.etp.v12.datatypes.message_header import MessageHeader

from etpproto.client_info import ClientInfo
from etpproto.error import (
    ETPError,
    InvalidMessageError,
    UnsupportedProtocolError,
    NotSupportedError,
    InvalidStateError,
    AuthorizationRequired,
)
from etpproto.messages import Message
from etpproto.utils import ProtocolDict, get_all_etp_protocol_classes

# import time


class Protocol:
    async def handle_message(
        self,
        etp_object: object,
        msg_header: MessageHeader,
        client_info: Union[None, ClientInfo] = None,
    ) -> AsyncGenerator[Optional[Message], None]:
        yield NotSupportedError().to_etp_message(
            correlation_id=msg_header.message_id
        )


class ConnectionType(Enum):
    """An enumeration of connection types."""

    #: This connection will act as client and talk to a remote server
    CLIENT = 1

    #: This connection will act as as server and waits for client connections
    SERVER = 2


class CommunicationProtocol(Enum):
    """
    ETP Specification, Section 2 - ETP Published Protocols
    """

    #: Creates and manages ETP sessions.
    CORE = 0
    #: Provides "simple streamer" functionality, for example, a sensor streaming data.
    CHANNEL_STREAMING = 1
    #: Gets channel data from a store in "rows".
    CHANNEL_DATA_FRAME = 2
    #: Enables store customers to enumerate and understand the contents of a store of data objects.
    DISCOVERY = 3
    #: Performs CRUD operations (create, retrieve, update and delete) on data objects in a store.
    STORE = 4
    #: Allows store customers to receive notification of changes to data objects in the store in an event-driven manner, resulting from operations in Protocol 4.
    STORE_NOTIFICATION = 5
    #: Manages the growing parts of data objects that are index-based (i.e., time and depth) other than channels.
    GROWING_OBJECT = 6
    #: Allows a store customer to receive notifications of changes to the growing parts of growing data objects in a store, in an event-driven manner, resulting from operations in Protocol 6.
    GROWING_OBJECT_NOTIFICATION = 7
    #: Transfers large, binary arrays of heterogeneous data values, which Energistics domain standards typically store using HDF5.
    DATA_ARRAY = 9
    #: Query behavior appended to discovery functionality (which is defined in Protocol 3).
    DISCOVERY_QUERY = 13
    #: Query behavior appended to store/CRUD functionality (which is defined in Protocol 4).
    STORE_QUERY = 14
    #: Query behavior appended to growing object behavior (which is defined in Protocol 6).
    GROWING_OBJECT_QUERY = 16
    #: Handles messages associate with software application transactions, for example, end messages for applications that may have long, complex transactions (typically associated with earth modeling/RESQML).
    TRANSACTION = 18
    #: Provides standard publish/subscribe behavior for customers to connect to a store (server) and receive new channel data as available (streaming).
    CHANNEL_SUBSCRIBE = 21
    #: Enables one server (with the ETP customer role) to push (stream) data to another server (with the ETP store role).
    CHANNEL_DATALOAD = 22
    #: Used to discover dataspaces in a store. After discovering dataspaces, use Discovery (Protocol 3) to discover objects in the store.
    DATASPACE = 24
    #: Enables store customers to discover a store's data model, to dynamically understand what object types are possible in the store at a given location in the data model (though the store may have no data for these object types), without prior knowledge of the overall data model and graph connectivity.
    SUPPORTED_TYPES = 25
    #: In ETP v1.1, this protocol was published as Protocol 8. It is now a custom protocol published by an Energistics member company. .
    WITSML_SOAP = 2000


@dataclass
class ETPConnection:
    """
    Main class to start an ETP Connection.
    :cvar generic_transition_table:
    :ivar server_capabilities: The capabilities of the current ETP connection (client or server)
    :ivar transition_table: The table that maps a communication protocol to an actual protocol implementation
    :ivar is_connected:
    :ivar chunk_msg_cache: Mapping a msgId to list of partial msg (). Is is ONLY used for RECIEVED messages
    """

    SUB_PROTOCOL: Final[str] = "etp12.energistics.org"

    generic_transition_table: ClassVar[
        ProtocolDict
    ] = get_all_etp_protocol_classes()

    transition_table: ClassVar[Dict[CommunicationProtocol, Protocol]] = {}

    server_capabilities: Optional[ServerCapabilities] = field(default=None)

    is_connected: bool = field(default=False)

    auth_required: bool = field(default=False)

    message_id: int = field(default=1)

    #    ______           __                                      __             ________                __
    #   / ____/___ ______/ /_  ___     ____  ____  __  _______   / /__  _____   / ____/ /_  __  ______  / /_______   ____ ___  ___  ______________ _____ ____
    #  / /   / __ `/ ___/ __ \/ _ \   / __ \/ __ \/ / / / ___/  / / _ \/ ___/  / /   / __ \/ / / / __ \/ //_/ ___/  / __ `__ \/ _ \/ ___/ ___/ __ `/ __ `/ _ \
    # / /___/ /_/ / /__/ / / /  __/  / /_/ / /_/ / /_/ / /     / /  __(__  )  / /___/ / / / /_/ / / / / ,< (__  )  / / / / / /  __(__  |__  ) /_/ / /_/ /  __/
    # \____/\__,_/\___/_/ /_/\___/  / .___/\____/\__,_/_/     /_/\___/____/   \____/_/ /_/\__,_/_/ /_/_/|_/____/  /_/ /_/ /_/\___/____/____/\__,_/\__, /\___/
    #                              /_/                                                                                                           /____/

    chunk_msg_cache: Dict[int, List[Message]] = field(
        default_factory=lambda: {}
    )

    error_msg_cache: Dict[int, List[Optional[Message]]] = field(
        default_factory=lambda: {}
    )

    client_info: ClientInfo = field(default=ClientInfo())

    connection_type: ConnectionType = field(default=ConnectionType.SERVER)
    # TODO : last msg recieve date
    # TODO : max timeout before disconnecting

    def __post_init__(self):
        if self.connection_type == ConnectionType.SERVER:
            self.message_id = 1
        elif self.connection_type == ConnectionType.CLIENT:
            self.message_id = 2
            self.auth_required = False  # auth is only required on server side

    def _handle_answer_and_error(
        self,
        msg: Optional[Union[Message, ETPError]],
        req_msg: Message,
        request_msg_id: int,
    ) -> Optional[Message]:
        # Si on a repondu un message c'est que tout s'est bien passé
        if msg:
            if isinstance(msg, Message):
                msg.header.message_id = self.consume_msg_id()
                msg.set_final_msg(True)
                return msg

            # conversion de l'erreur en msg etp si besoin
            elif isinstance(msg, ETPError):
                return msg.to_etp_message(request_msg_id)
        return None

    async def _handle_message_generator(
        self, etp_input_msg: Optional[Message]
    ) -> AsyncGenerator[Optional[Message], None]:
        if (
            etp_input_msg is not None and etp_input_msg.header is not None
        ):  # si pas un message none
            if not self.auth_required or (
                self.client_info is not None
                and (
                    self.client_info.authenticated
                    or (
                        isinstance(etp_input_msg.body, Authorize)
                        or isinstance(etp_input_msg.body, AuthorizeResponse)
                    )
                )
            ):
                if (
                    # isinstance(etp_input_msg.body, RequestSession)
                    # or isinstance(etp_input_msg.body, OpenSession)
                    etp_input_msg.header.protocol
                    == CommunicationProtocol.CORE.value
                    or self.is_connected
                ):
                    current_msg_id = etp_input_msg.header.message_id

                    # if requires acknowledge :
                    if (
                        etp_input_msg.is_asking_acknowledge()
                        and not isinstance(etp_input_msg.body, Acknowledge)
                    ):
                        yield Message.get_object_message(
                            Acknowledge(),
                            correlation_id=current_msg_id,
                            msg_id=self.consume_msg_id(),
                        )
                        # time.sleep(3)

                    # only if the user is connected or request for an OpenSession or if the message is not the full message

                    if self.is_connected and isinstance(
                        etp_input_msg.body, CloseSession
                    ):
                        logging.debug(
                            f"{self.client_info.ip} : CloseSession recieved"
                        )
                        self.is_connected = False
                    else:
                        # Test if it is an Open/Request session
                        if (
                            isinstance(etp_input_msg.body, RequestSession)
                            and self.connection_type == ConnectionType.SERVER
                        ) or (
                            isinstance(etp_input_msg.body, OpenSession)
                            and self.connection_type == ConnectionType.CLIENT
                        ):
                            self.is_connected = True
                            self.client_info.negotiate(etp_input_msg.body)

                        # logging.debug(etp_input_msg, etp_input_msg.is_chunk_msg(), etp_input_msg.is_chunk_msg_referencer())
                        # On test si c'est un message de BLOB qu'il faut mettre en cache :
                        if etp_input_msg.is_multipart_msg() and (
                            etp_input_msg.is_chunk_msg()
                            or etp_input_msg.is_chunk_msg_referencer()
                        ):
                            cache_id = (
                                etp_input_msg.header.correlation_id
                                if etp_input_msg.header.correlation_id != 0
                                else etp_input_msg.header.message_id
                            )
                            if cache_id not in self.chunk_msg_cache:
                                self.chunk_msg_cache[cache_id] = []
                            self.chunk_msg_cache[cache_id].append(
                                etp_input_msg
                            )

                            # si final on rassemble et on handle.
                            if etp_input_msg.is_final_msg():
                                logging.debug(
                                    f"Reassemble chunks :{self.chunk_msg_cache[cache_id]}",
                                )
                                try:
                                    async for msg in self._handle_message_generator(
                                        Message.reassemble_chunk(
                                            self.chunk_msg_cache[cache_id]
                                        )
                                    ):
                                        if msg is not None:
                                            yield msg
                                        else:
                                            if (
                                                cache_id
                                                not in self.error_msg_cache
                                            ):
                                                self.error_msg_cache[
                                                    cache_id
                                                ] = []
                                            self.error_msg_cache[
                                                cache_id
                                            ].append(
                                                InvalidMessageError().to_etp_message(
                                                    msg_id=self.consume_msg_id()
                                                )
                                            )

                                except Exception as e:
                                    logging.error(
                                        f"{self.client_info.ip}: _SERVER_ not handled exception",
                                    )
                                    raise e

                                if cache_id in self.error_msg_cache:
                                    for err_msg in self.error_msg_cache[
                                        cache_id
                                    ]:
                                        if err_msg is not None:
                                            yield err_msg
                                    self.error_msg_cache.pop(cache_id)

                                if cache_id in self.chunk_msg_cache:
                                    self.chunk_msg_cache.pop(cache_id)

                        else:  # ce n'est pas un message envoye en chunks
                            # now try to have an answer
                            try:
                                # Test si le protocol est supporte par le serveur
                                if (
                                    CommunicationProtocol(
                                        etp_input_msg.header.protocol
                                    )
                                    in self.transition_table
                                ):
                                    # demande la reponse au protocols du serveur
                                    try:
                                        async for handled in self.transition_table[
                                            CommunicationProtocol(
                                                etp_input_msg.header.protocol
                                            )
                                        ].handle_message(
                                            etp_object=etp_input_msg.body,
                                            msg_header=etp_input_msg.header,
                                            client_info=self.client_info,
                                        ):
                                            yield self._handle_answer_and_error(
                                                msg=handled,
                                                req_msg=etp_input_msg,
                                                request_msg_id=current_msg_id,
                                            )
                                    except ETPError as exp_invalid_msg_type:
                                        yield exp_invalid_msg_type.to_etp_message(
                                            msg_id=self.consume_msg_id(),
                                            correlation_id=current_msg_id,
                                        )
                                else:
                                    logging.debug(
                                        f"{self.client_info.ip} : #handle_msg : unkown protocol id : {str(etp_input_msg.header.protocol)}"
                                    )
                                    raise UnsupportedProtocolError(
                                        etp_input_msg.header.protocol
                                    )
                            except ETPError as etp_err:
                                logging.error(
                                    f"{self.client_info.ip}: _SERVER_ internal error : {etp_err}"
                                )
                                yield self._handle_answer_and_error(
                                    msg=etp_err.to_etp_message(
                                        msg_id=self.consume_msg_id(),
                                        correlation_id=current_msg_id,
                                    ),
                                    req_msg=etp_input_msg,
                                    request_msg_id=current_msg_id,
                                )
                            except Exception as e:
                                logging.error(
                                    f"{self.client_info.ip}: _SERVER_ not handled exception",
                                )
                                raise e
                else:  # not connected
                    yield InvalidStateError().to_etp_message(
                        msg_id=self.consume_msg_id()
                    )
            else:  # not authenticated
                yield AuthorizationRequired().to_etp_message(
                    msg_id=self.consume_msg_id()
                )
        else:  # null message
            yield InvalidMessageError().to_etp_message(
                msg_id=self.consume_msg_id()
            )

    async def send_msg_and_error_generator(
        self, msg: Message, err_msg: Message
    ) -> AsyncGenerator[Tuple[int, bytes], None]:
        current_msg_id = self.consume_msg_id()

        if msg:
            # Attention, le flag (FIN bit) ne doit etre mis que sur le dernier msg
            msg.header.message_flags = msg.header.message_flags | 0x02
            msg.header.message_id = current_msg_id

            # err_msg.header.message_id = self.message_id
            # self.message_id += 2

            async for msg_part in msg.encode_message_generator(
                self.client_info.getCapability(
                    "MaxWebSocketMessagePayloadSize"
                ),
                self,
            ):
                yield (current_msg_id, msg_part)

            if err_msg:
                async for msg_part in err_msg.encode_message_generator(
                    self.client_info.getCapability(
                        "MaxWebSocketMessagePayloadSize"
                    ),
                    self,
                ):
                    yield (current_msg_id, msg_part)

        elif err_msg:
            # logging.debug(f"{self.client_info.ip} : Encoding error")
            async for msg_part in err_msg.encode_message_generator(
                self.client_info.getCapability(
                    "MaxWebSocketMessagePayloadSize"
                ),
                self,
            ):
                yield (current_msg_id, msg_part)

    async def handle_bytes_generator(
        self, msg_data: bytes
    ) -> AsyncGenerator[bytes, None]:
        """
        Returns a generator of binary messages to send
        """

        etp_input_msg = Message.decode_binary_message(
            msg_data, ETPConnection.generic_transition_table
        )
        logging.debug(f"### MSG {etp_input_msg}")

        async for msg in self._handle_message_generator(etp_input_msg):
            if msg is not None:
                async for msg_part in msg.encode_message_generator(
                    self.client_info.getCapability(
                        "MaxWebSocketMessagePayloadSize"
                    ),
                    self,
                ):
                    yield msg_part

    def consume_msg_id(self):
        tmp_msg_id = self.message_id
        self.message_id += 2
        return tmp_msg_id

    @classmethod
    def on(
        cls: Type[ETPConnection], protocol: CommunicationProtocol
    ) -> Callable[[Type[Protocol]], Type[Protocol]]:
        """Should only be used to decorate classes."""

        def decorate(cls_protocol: Type[Protocol]) -> Type[Protocol]:
            cls.transition_table[protocol] = cls_protocol()
            return cls_protocol

        return decorate

    @classmethod
    def dec_server_capabilities(cls: Type[ETPConnection]):
        """Should only be used to decorate classes."""

        def decorate(compute_capability):
            cls.server_capabilities = compute_capability(
                cls.get_supported_protocol_list
            )
            return compute_capability

        return decorate

    @classmethod
    def get_supported_protocol_list(
        cls: Type[ETPConnection],
    ) -> List[SupportedProtocol]:
        supported_protocols: List[SupportedProtocol] = []
        for protocol in cls.transition_table:
            if protocol.value != CommunicationProtocol.CORE:
                supported_protocols.append(
                    SupportedProtocol(
                        protocol=protocol.value,
                        protocol_version={
                            "major": 1,
                            "minor": 2,
                            "patch": 0,
                            "revision": 0,
                        },
                        role="server",
                        protocol_capabilities={},
                    )
                )
        return supported_protocols
