# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

import sys
from dataclasses import dataclass, field
from typing import ClassVar, List, Optional, Union


@dataclass
class EndpointCapabilityKind:

    _default: ClassVar[Optional[Union[int, str, float, bool]]] = None
    _min: ClassVar = None
    _max: ClassVar = None
    _unit: ClassVar[Optional[str]] = None


@dataclass
class ActiveTimeoutPeriod(EndpointCapabilityKind):
    """
    The minimum time period in seconds that a store keeps the active status (activeStatus field in ETP) for a data object as “active”, after the most recent update causing the data object’s active status to be set to true. For growing data objects, this is any change to its parts. For channels, this is any change to its data points.
    This capability can be set for an endpoint and/or for a data object. A data object-specific value overrides an endpoint-specific value.
    """

    _default = 3600
    _min = 60
    _max = None
    _unit = "seconds, <count of seconds>"
    value: Optional[int] = field(default=3600)


@dataclass
class AuthorizationDetails(EndpointCapabilityKind):
    """
    1. Contains an ArrayOfString with WWW-Authenticate style challenges.
    2. To support the required authorization workflow (to enable an endpoint to acquire an access token with the necessary scope from the designated authorization server), the AuthorizationDetails endpoint capability MUST include at least one challenge with the Bearer scheme which must include the ‘authz_server' and ‘scope’ parameters.
    a. The 'authz_server' parameter MUST be a URI for an authorization server to enable the endpoint to acquire any other needed metadata about the authorization server using OpenID Connect Discovery.
    3. An ETP server MUST have the AuthorizationDetails endpoint capability, which must meet the requirements of Point 2 above.
    4. If an ETP client does NOT need to authorize ETP servers, it MAY omit the AuthorizationDetails.
    """

    _default = None
    _min = None
    _max = None
    _unit = None
    value: Optional[List[str]] = field(default=None)


@dataclass
class ChangePropagationPeriod(EndpointCapabilityKind):
    """
    The maximum time period in seconds--under normal operation on an uncongested session--for these conditions:
        > after a change in an endpoint before that endpoint sends a change notification covering the change to any subscribed endpoint in any session.
        > if the change was the result of a message WITHOUT a positive response, it is the maximum time until the change is reflected in read operations in any session.
        > If the change was the result of a message WITH a positive response, it is the maximum time until the change is reflected in sessions other than the session where the change was made. RECOMMENDATION: Set as short as possible (i.e. a few seconds).
    """

    _default = 5
    _min = 1
    _max = 600
    _unit = "seconds, <count of seconds>"
    value: Optional[int] = field(default=5)


@dataclass
class ChangeRetentionPeriod(EndpointCapabilityKind):
    """
    The minimum time period in seconds that a store retains the Canonical URI of a deleted data object and any change annotations for channels and growing objects. RECOMMENDATION: This period should be as long as is feasible in an implementation. When the period is shorter, the risk is that additional data will need to be transmitted to recover from outages, leading to higher initial load on sessions.
    """

    _default = 86400
    _min = 86400
    _max = None
    _unit = "seconds, <count of seconds>"
    value: Optional[int] = field(default=86400)


@dataclass
class MaxConcurrentMultipart(EndpointCapabilityKind):
    """
    The maximum count of multipart messages allowed in parallel, in a single protocol, from one endpoint to another. The limit applies separately to each protocol, and separately from client to server and from server to client. The limit for an endpoint applies to the multipart messages that the endpoint can receive.
    EXAMPLE: If an endpoint's MaxConcurrentMultipart is 5, then it can receive 5 messages--each with any number of parts--at one time, in Store (Protocol 4) and another 5 messages in process in Discovery (Protocol 3). In Discovery (Protocol 3), this could be the multipart responses to 5 distinct GetResources request messages.
    """

    _default = None
    _min = 1
    _max = None
    _unit = "count, <count of messages>"
    value: Optional[int] = field(default=None)


@dataclass
class MaxDataObjectSize(EndpointCapabilityKind):
    """
    The maximum size in bytes of a data object allowed in a complete multipart message. Size in bytes is the size in bytes of the uncompressed string representation of the data object in the format in which it is sent or received.
    This capability can be set for an endpoint, a protocol, and/or a data object. If set for all three, here is how they generally work:
        > An object-specific value overrides an endpoint-specific value.
        > A protocol-specific value can further lower (but NOT raise) the limit for the protocol.
    EXAMPLE: A store may wish to generally support sending and receiving any data object that is one megabyte or less with the exceptions of Wells that are 100 kilobytes or less and attachments that are 5 megabytes or less. A store may further wish to limit the size of any data object sent as part of a notification in StoreNotification (Protocol 5) to 256 kilobytes.
    """

    _default = None
    _min = 100000
    _max = None
    _unit = "bytes"
    value: Optional[int] = field(default=None)


@dataclass
class MaxSessionClientCount(EndpointCapabilityKind):
    """
    The maximum count of concurrent ETP sessions that may be established for a given endpoint, by a specific client. If possible, the determination of whether this limit is exceeded should be made at the time of receiving the HTTP WebSocket upgrade or connect request based on the authorization details provided with the request. At the latest, it should be based on an authorized RequestSession message.
    """

    _default = None
    _min = 2
    _max = None
    _unit = "count, <count of sessions>"
    value: Optional[int] = field(default=None)


@dataclass
class MaxPartSize(EndpointCapabilityKind):
    """
    The The maximum size in bytes of each data object part allowed in a standalone message or a complete multipart message. Size in bytes is the total size in bytes of the uncompressed string representation of the data object part in the format in which it is sent or received.
    """

    _default = None
    _min = 100000
    _max = None
    _unit = "bytes, <number of bytes>"
    value: Optional[int] = field(default=None)


@dataclass
class MaxSessionGlobalCount(EndpointCapabilityKind):
    """
    The maximum count of concurrent ETP sessions that may be established for a given endpoint across all clients. The determination of whether this limit is exceeded should be made at the time of receiving the HTTP WebSocket upgrade or connect request. NOTE: Exposing this information may have security implications, so it should only be exposed if an implementation is comfortable with any potential associated risks.
    """

    _default = None
    _min = 2
    _max = None
    _unit = "count, <count of sessions>"
    value: Optional[int] = field(default=None)


@dataclass
class MaxWebSocketFramePayloadSize(EndpointCapabilityKind):
    """
    The maximum size in bytes allowed for a single WebSocket frame payload. The limit to use during a session is the smaller of the client's and the server's value for MaxWebSocketFramePayloadSize, which should be determined by the limits imposed by the WebSocket library used by each endpoint.
    """

    _default = None
    _min = None
    _max = None
    _unit = "bytes, <number of bytes>"
    value: Optional[int] = field(default=None)


@dataclass
class MaxWebSocketMessagePayloadSize(EndpointCapabilityKind):
    """
    The maximum size in bytes allowed for a complete WebSocket message payload, which is composed of one or more WebSocket frames. The limit to use during a session is the smaller of the client's and the server's value for MaxWebSocketMessagePayloadSize, which should be determined by the limits imposed by the WebSocket library used by each endpoint.
    """

    _default = None
    _min = None
    _max = None
    _unit = "bytes, <number of bytes>"
    value: Optional[int] = field(default=None)


@dataclass
class MultipartMessageTimeoutPeriod(EndpointCapabilityKind):
    """
    The maximum time period in seconds--under normal operation on an uncongested session--allowed between subsequent messages in the SAME multipart request or response. The period is measured as the time between when each message has been fully sent or received via the WebSocket.
    """

    _default = None
    _min = 60
    _max = None
    _unit = "seconds, <count of seconds>"
    value: Optional[int] = field(default=None)


@dataclass
class ResponseTimeoutPeriod(EndpointCapabilityKind):
    """
    The maximum time period in seconds allowed between a request and the standalone response message or the first message in the multipart response message. The period is measured as the time between when the request message has been successfully sent via the WebSocket and when the first or only response message has been fully received via the WebSocket. When calculating this period, any Acknowledge messages or empty placeholder responses are ignored EXCEPT where these are the only and final response(s) to the request.
    """

    _default = 300
    _min = 60
    _max = None
    _unit = "seconds, <count of seconds>"
    value: Optional[int] = field(default=300)


@dataclass
class RequestSessionTimeoutPeriod(EndpointCapabilityKind):
    """
    The maximum time period in seconds a server will wait to receive a RequestSession message from a client after the WebSocket connection has been established.
    """

    _default = 45
    _min = 5
    _max = None
    _unit = "seconds, <count of seconds>"
    value: Optional[int] = field(default=45)


@dataclass
class SessionEstablishmentTimeoutPeriod(EndpointCapabilityKind):
    """
    The maximum time period in seconds a client or server will wait for a valid ETP session to be established.
    For a server:
        > A valid session is established when it sends an OpenSession message to the client, which indicates a session has been successfully established.
        > The time period starts when it receives the initial RequestSession message from the client.
    For a client:
        > A valid session is established when it receives an OpenSession message from the server.
        > The time period starts when it sends the initial RequestSession message to the server.
    """

    _default = 60
    _min = 5
    _max = None
    _unit = "seconds, <count of seconds>"
    value: Optional[int] = field(default=60)


@dataclass
class SupportsAlternateRequestUris(EndpointCapabilityKind):
    """
    Indicates whether an endpoint supports alternate URI formats--beyond the canonical Energistics URIs, which MUST be supported for requests.
    """

    _default = False
    _unit = None
    value: Optional[bool] = field(default=False)


@dataclass
class SupportsMessageHeaderExtensions(EndpointCapabilityKind):
    """
    Indicates whether an endpoint supports message header extensions. For more information about message header extensions and their use, see Section 3.6.2.
    """

    _default = False
    _unit = None
    value: Optional[bool] = field(default=False)


def kind_from_name(classname):
    return getattr(sys.modules[__name__], classname)
