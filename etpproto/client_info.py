# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0
import logging

from typing import Any, ClassVar, Dict, Union

from dataclasses import dataclass, field

from etptypes.energistics.etp.v12.protocol.core.open_session import OpenSession
from etptypes.energistics.etp.v12.protocol.core.request_session import (
    RequestSession,
)

from etpproto.endpoint_capability_kind import kind_from_name


@dataclass
class ClientInfo:
    count_instance: ClassVar[int] = 0

    endpoint_capabilities: Dict[str, Any] = field(
        default_factory=lambda: {
            "MaxWebSocketFramePayloadSize": 10000,
            "MaxWebSocketMessagePayloadSize": 10000,
        }
    )
    login: str = field(default="anonymousUser")
    ip: str = field(default="0.0.0.0")
    authenticated: bool = field(default=False)

    def __post_init__(self):
        self._id = self.count_instance
        ClientInfo.count_instance = ClientInfo.count_instance + 1

    def getCapability(self, name: str) -> Any:
        if name in self.endpoint_capabilities:
            return self.endpoint_capabilities[name]
        return None

    def negotiate(self, msg: Union[OpenSession, RequestSession]):
        for k, v in msg.endpoint_capabilities.items():
            cap_class = kind_from_name(k)
            # if cap_class != None:
            val: Any = v
            if hasattr(val, "item"):
                val = v.item  # if it is a DataValue

            if k in self.endpoint_capabilities:
                try:
                    val = min(val, self.endpoint_capabilities[k])
                except Exception as e1:
                    logging.debug(e1)

            if cap_class is not None:
                if cap_class._min is not None:
                    val = max(val, cap_class._min)
                if cap_class._max is not None:
                    val = min(val, cap_class._max)

            self.endpoint_capabilities[k] = val

        logging.debug(f"Negotiated capa : {self.endpoint_capabilities}")
        # else:
        # logging.debug(f"No capability found for name '{k}'")

    def __str__(self) -> str:
        return (
            "ClientInfo["
            + str(self._id)
            + "] "
            + self.ip
            + "<"
            + str(self.login)
            + ">"
        )

    def __hash__(self) -> int:
        return self._id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ClientInfo):
            return NotImplemented
        return self._id == other._id
