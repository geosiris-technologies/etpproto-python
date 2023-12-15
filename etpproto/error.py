# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

from typing import ClassVar, Optional

from etptypes.energistics.etp.v12.datatypes.error_info import ErrorInfo
from etptypes.energistics.etp.v12.protocol.core.protocol_exception import (
    ProtocolException,
)

from etpproto.messages import Message


# https://stackoverflow.com/a/25004536


class ETPError(Exception):
    """Base class for ETP exceptions"""

    code: ClassVar[int] = 0

    def __init__(self, msg: str) -> None:
        super().__init__()
        self.msg = msg

    def to_etp_error(self) -> ErrorInfo:
        return ErrorInfo(message=str(self.msg), code=self.code)
        # return ErrorInfo(message=str(self), code=self.code)

    def to_etp_message(
        self, msg_id: int = -1, correlation_id: int = 0
    ) -> Optional[Message]:
        err_info = self.to_etp_error()
        return Message.get_object_message(
            ProtocolException(error=err_info, errors={}),
            msg_id=msg_id,
            correlation_id=correlation_id,
        )


class NoRoleError(ETPError):
    code: ClassVar[int] = 1

    def __init__(self) -> None:
        super().__init__("The endpoint does not support the requested role.")


class NoSupportedProtocolsError(ETPError):
    code: ClassVar[int] = 2

    def __init__(self) -> None:
        super().__init__(
            "The server does not support any of the requested protocols."
        )


class InvalidMessageTypeError(ETPError):
    code: ClassVar[int] = 3

    def __init__(self) -> None:
        super().__init__(
            "The message type ID is either: 1) not defined at all in the ETP Specification (e.g., no schema for it); or 2) not a correct message type ID for the receiving role (EXAMPLE: Per this specification, only the store role may SEND a GetDataObjectsResponse message; if the store RECEIVES a GetDataObjectsResponse message, it MUST send this error code.)",
        )


class UnsupportedProtocolError(ETPError):
    code: ClassVar[int] = 4

    def __init__(self, protocol_id: int) -> None:
        super().__init__(
            f"The endpoint does not support the protocol (#{protocol_id}) identified in a message header.",
        )


class InvalidArgumentError(ETPError):
    """Use this error code in any situation where a logically invalid argument is encountered."""

    code: ClassVar[int] = 5

    def __init__(self) -> None:
        super().__init__("Logically invalid argument.")


class RequestDeniedError(ETPError):
    """
    RECOMMENDATION: Endpoints should supply an error message explaining why the request was denied.
    For example, for read-only servers (which do not support Store operations),
    the explanation could be "Read-only server; operation not supported.
    """

    code: ClassVar[int] = 6

    def __init__(self, more: str) -> None:
        super().__init__("Server has denied the request. " + more)


class NotSupportedError(ETPError):
    code: ClassVar[int] = 7

    def __init__(self) -> None:
        super().__init__("The operation is not supported by the endpoint.")


class InvalidStateError(ETPError):
    """
    Indicates that the message is not allowed in the current state of the protocol.
    For example, sending ChannelStreamingStart for a channel that is already streaming,
    or received a message that is not applicable for the current role.
    """

    code: ClassVar[int] = 8

    def __init__(self) -> None:
        super().__init__(
            "The message is not allowed in the current state of the protocol.",
        )


class InvalidUriError(ETPError):
    """
    EXAMPLE: If a customer sends an alternate URI format to a store that does not accept/support
    alternate URIs, the store MUST send this error code.
    """

    code: ClassVar[int] = 9

    def __init__(self) -> None:
        super().__init__(
            "The URI sent is either a malformed URI or is not a valid URI format for ETP.",
        )


class ExpiredTockenError(ETPError):
    """
    Sent from server to client when the server is about to terminate the session because of an expired security token.
    """

    code: ClassVar[int] = 10

    def __init__(self) -> None:
        super().__init__("The security token is expired.")


class NotFoundError(ETPError):
    """
    Used when a resource (i.e., an object, part or range) is not found.
    """

    code: ClassVar[int] = 11

    def __init__(self) -> None:
        super().__init__("Resource not found.")


class LimitExceededError(ETPError):
    """
    Sent by a store if a request exceeds allowed limits or what the endpoint can handle. For example, this error code is used:
        - In Protocol 3 (Discover) and all query protocols, if the results of a client request exceeds the MaxResponseCount variable (which indicates the maximum number of resources a store will return).
        - In Protocol 21 (ChannelSubscribe) if a producer exceeds a consumers MaxDataItemCount.
    """

    code: ClassVar[int] = 12

    def __init__(self) -> None:
        super().__init__("Request exceeds allowed limits.")


class CompressionNotSupportedError(ETPError):
    """
    Sent by any role (producer, consumer, etc.) when it receives one of the following message types, which can never be compressed: RequestSession, OpenSession, ProtocolException or Acknowledge.
    """

    code: ClassVar[int] = 13

    def __init__(self) -> None:
        super().__init__("Message can not be compressed.")


class InvalidObjectError(ETPError):
    """
    Sent in any protocol when either role sends an invalid XML document. Note: ETP does not distinguish between well-formed and invalid for this purpose. The same error message is used in both cases.
    """

    code: ClassVar[int] = 14

    def __init__(self) -> None:
        super().__init__("Invalid XML document.")


class MaxTransactionsExceededError(ETPError):
    """
    The maximum number of transactions per ETP session has been exceeded. Currently, Transaction (Protocol 18) is the only ETP protocol that has the notion of a "transaction" and allows only 1 transaction per session.
    """

    code: ClassVar[int] = 15

    def __init__(self) -> None:
        super().__init__(
            "Maximum number of transactions per ETP session has been exceeded.",
        )


class ContentTypeNotSupportedError(ETPError):
    """
    The content type is not supported by the server.
    """

    code: ClassVar[int] = 16

    def __init__(self) -> None:
        super().__init__("The content type is not supported by the server.")


class MaxSizeExceededError(ETPError):
    """
    Sent from a store to a customer when the customer attempts a get or put operation that exceeds the stores maximum advertised MaxDataObjectSize, MaxPartSize, or MaxDataArraySize.
    """

    code: ClassVar[int] = 17

    def __init__(self) -> None:
        super().__init__(
            "Operation exceeds the stores maximum advertised MaxDataObjectSize, MaxPartSize, or MaxDataArraySize.",
        )


class MultipartCancelledError(ETPError):
    """
    Sent by either role to notify of canceled transmission of multi-message response or request when one of the maximum advertised protocol capabilities (maxConcurrentMultipart, maxMultipartMessageTimeInterval, or maxMultipartTotalSize) has been exceeded.
    """

    code: ClassVar[int] = 18

    def __init__(self) -> None:
        super().__init__("Canceled transmission of multi-message response.")


class InvalidMessageError(ETPError):
    """
    Sent by either endpoint when it is unable to deserialize the header or body of a message.
    """

    code: ClassVar[int] = 19

    def __init__(self) -> None:
        super().__init__(
            "Unable to deserialize the header or body of a message."
        )


class InvalidIndexKindError(ETPError):
    """
    Sent by either role when an IndexKind used in a message is invalid for the dataset. For example, see the Replace Range message in ChannelDataLoad (Protocol 22).
    """

    code: ClassVar[int] = 20

    def __init__(self) -> None:
        super().__init__(
            "IndexKind used in message is invalid for the dataset."
        )


class NoSupportedFormatsError(ETPError):
    """
    Sent by either role if, during session negotiation, no agreement can be reached on the format (XML or JSON) of data objects. The role that sends this message should then send the CloseSession message.
    """

    code: ClassVar[int] = 21

    def __init__(self) -> None:
        super().__init__(
            "No agreement can be reached on the format (XML or JSON) of data objects.",
        )


class RequestUuidRejectedError(ETPError):
    """
    Sent by the store when it rejects a customer-assigned request UUID (requestUuid), most likely because the request UUID is not unique within the session.
    """

    code: ClassVar[int] = 22

    def __init__(self) -> None:
        super().__init__(
            "Rejects a customer-assigned request UUID (requestUuid)."
        )


class UpdateGrowingObjectDeniedError(ETPError):
    """
    Sent by a store when a customer tries to update an existing growing object (i.e., do a put operation) using Store (Protocol 4). Growing objects can only be updated using GrowingObject (Protocol 6).
    """

    code: ClassVar[int] = 23

    def __init__(self) -> None:
        super().__init__(
            "Tryed to update an existing growing object using Store (Protocol 4).",
        )


class BackPressureLimitExceededError(ETPError):
    """
    Indicates the sender has detected the receiver is not processing messages as fast as it can send them and exceeding its capacity in its outgoing buffers. If sender capacity is exhausted and it is eas
    """

    code: ClassVar[int] = 24

    def __init__(self) -> None:
        super().__init__("Receiver's outgoing buffers capacity exceeded.")


class BackPressureWarningError(ETPError):
    """
    Listen to recording; at what criteria do you send the warming.
    """

    code: ClassVar[int] = 25

    def __init__(self) -> None:
        super().__init__("Back Pressure Warning.")


class TimedOut(ETPError):
    """
    May be sent by either role to cancel an operation when the response time for a relevant operation is exceeded, such as ResponseTimeoutPeriod or MultipartMessageTimeoutPeriod capabilities.
    """

    code: ClassVar[int] = 26

    def __init__(self) -> None:
        super().__init__("Response timeout")


class AuthorizationRequired(ETPError):
    """
    Sent from an endpoint during session negotiation (and ONLY during session negotiation) to indicate that the other endpoint requires authorization.
    """

    code: ClassVar[int] = 27

    def __init__(self) -> None:
        super().__init__("Authorization required.")


class AuthorizationExpiring(ETPError):
    """
        Optionally sent from an endpoint when the other endpoint's authorization will expire soon. The receiving endpoint should follow the necessary authorization workflow to renew its authorization. If it does not, the sending endpoint will eventually terminate the connection.
    The precise definition of "soon" and the required re-authorization workflow are intentionally out of the scope of the ETP Specification.
    """

    code: ClassVar[int] = 28

    def __init__(self) -> None:
        super().__init__("Authorization expiring.")


class NoSupportedDataObjectTypes(ETPError):
    """
    The server does not support any of the client's supported data object types.
    """

    code: ClassVar[int] = 29

    def __init__(self) -> None:
        super().__init__(
            "The server does not support any of the client's supported data object types."
        )


class ResponseCountExceeded(ETPError):
    """
    Sent by a store endpoint to terminate a non-map response once the number of responses sent has reached the allowed or stated limits specified by the relevant capabilities. This lets customers know that the store has more data than it could return to the customer. EXAMPLES:
        - In Protocol 3 (Discovery) and all query protocols, sent by the store if it must stop sending responses to the customer because it has already sent MaxResponseCount responses to a customer request.
        - In Protocol 21 (ChannelSubscribe), sent by the store if it must stop sending data points to a customer in response to a GetRanges request because the store has already sent MaxRangeDataItemCount data points in response to the request.
    """

    code: ClassVar[int] = 30

    def __init__(self) -> None:
        super().__init__("Response count exceeded.")


class InvalidAppend(ETPError):
    """
    Sent in response to a ChannelData message that is not appending data to a channel.
    """

    code: ClassVar[int] = 31

    def __init__(self) -> None:
        super().__init__(
            "Sent in response to a ChannelData message that is not appending data to a channel."
        )


class InvalidOperation(ETPError):
    """
    Sent in response to a request when the requested operation would be invalid. EXAMPLE: In Protocol 6 (GrowingObject), a ReplacePartsByRange message where some replacement parts are not covered by the delete range is an invalid operation.
    """

    code: ClassVar[int] = 32

    def __init__(self) -> None:
        super().__init__("Invalid operation.")


class InvalidChannelIDError(ETPError):
    """
    Sent by either role  when operations are requested on a channel that does not exist.
    """

    code: ClassVar[int] = 1002

    def __init__(self) -> None:
        super().__init__(
            "Operations are requested on a channel that does not exist."
        )


class UnsupportedObjectError(ETPError):
    """
    Sent in the Store protocols, when either role sends or requests a data object type that is not supported, according to the protocol capabilities.
    """

    code: ClassVar[int] = 4001

    def __init__(self) -> None:
        super().__init__(
            "Operations are requested on a channel that does not exist."
        )


class NoCascadeDeleteError(ETPError):
    """
    Sent when an attempt is made to delete an object that has children and the store does not support cascading deletes.
    """

    code: ClassVar[int] = 4003

    def __init__(self) -> None:
        super().__init__("Store does not support cascading deletes.")


class PluralObjectError(ETPError):
    """
    Sent when an endpoint uses puts for more than one object under the plural root of a 1.x Energistics data object. ETP only supports a single data object, one XML document.
    """

    code: ClassVar[int] = 4004

    def __init__(self) -> None:
        super().__init__(
            "ETP only supports a single data object, one XML document."
        )


class GrowingPortionIgnoredError(ETPError):
    """
    Sent from a store to a customer when the customer supplies the growing portion in a Put. This is advisory only; the object is upserted, but the growing portion is ignored.
    """

    code: ClassVar[int] = 4005

    def __init__(self) -> None:
        super().__init__("Customer supplies the growing portion in a Put.")


class RetentionPeriodExceededError(ETPError):
    """
    Sent from a store to a customer when the client asks for changes beyond the stated change period of a server.
    """

    code: ClassVar[int] = 5001

    def __init__(self) -> None:
        super().__init__(
            "Ask for changes beyond the stated change period of a server.",
        )


class NotGrowingObjectError(ETPError):
    """
    Sent from a store to a customer when the customer attempts to perform a growing object operation on an object that is not defined as a growing object type. This message does NOT apply to an object declared as a growing object but that is simply not actively growing at the present time.
    """

    code: ClassVar[int] = 6001

    def __init__(self) -> None:
        super().__init__(
            "Growing object operation on an object that is not defined as a growing object type.",
        )


class InternalError(ETPError):
    """
    Sent when an error occured but is was not an ETP specific error
    """

    code: ClassVar[int] = -1
