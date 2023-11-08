# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging

import json
import re
import traceback
import uuid as pyUUID
from abc import ABC
from copy import deepcopy
from dataclasses import dataclass
from enum import IntFlag
from io import BytesIO
from math import ceil
from typing import AsyncGenerator, Dict, List, Optional, Tuple, Any, Union

import etptypes.energistics.etp.v12.datatypes.message_header as mh
from etptypes import ETPModel, avro_schema
from etptypes.energistics.etp.v12.datatypes.object.data_object import (
    DataObject,
)
from etptypes.energistics.etp.v12.datatypes.uuid import Uuid
from fastavro import schemaless_reader, schemaless_writer

from etpproto.utils import (
    ProtocolDict,
    concat,
    get_class_from_protocol_and_name,
    get_first_dict_attribute_name,
    get_first_list_attribute_name,
)


class MessageFlags(
    IntFlag
):  # enum.Flag class would be a better choice but doesn't work with our operations
    # None
    NONE = 0x0

    # A part of a multi-part message.
    MULTIPART = 0x1

    # The final part of a multi-part message.
    FINALPART = 0x2

    # Short-hand for both mutli-part and final part: 0x1 | 0x2
    MULTIPART_AND_FINALPART = 0x3

    # No data is available.
    NO_DATA = 0x4

    # The message body is compressed.
    COMPRESSED = 0x8

    # An Acknowledge message is requested by the sender.
    ACKNOWLEDGE = 0x10

    # The message has a header extension.
    HAS_HEADER_EXTENSION = 0x20


@dataclass
class Message(ABC):
    header: mh.MessageHeader
    body: ETPModel

    def encode_message(self) -> bytes:
        bio = BytesIO()
        if self.header:
            header_schema = json.loads(mh.avro_schema)
            schemaless_writer(
                bio, header_schema, self.header.dict(by_alias=True)
            )
        objSchema = json.loads(avro_schema(type(self.body)))
        schemaless_writer(bio, objSchema, self.body.dict(by_alias=True))

        value = bio.getvalue()

        return value

    # Encodage d'un message :
    #   Si le message depasse la taille max :
    #       - Si c'est un "Plural Message" (see. 3.7.3 de la documention etp v1.1)
    #           > Si oui on essaie de decouper la map en plusieurs messages
    #               +++> Verifier si le nb de clefs ne depassent pas le MaxResponseCount, sinon on envoie un ERESPONSECOUNT_EXCEEDED (see 3.7.3.1)
    #           > Sinon si contient un BLOB, on essaie de le couper en plusieurs messages
    #           > Sinon on envoie un message d'erreur
    #       - Sinon, on si contient un BLOB, on essaie de le couper en plusieurs messages
    #       - Sinon on envoie un message d'erreur
    # max_bytes_per_msg : negative number for infinite size
    async def encode_message_generator(  # noqa
        self, max_bytes_per_msg, connection
    ) -> AsyncGenerator[bytes, None]:
        # si requete on met le correlation_id sur le l'id du premier message
        # si self a un correlation_id alors c'est que ce n'était pas le premier
        correlation_id = (
            self.header.correlation_id
            if self.header.correlation_id != 0
            else self.header.message_id
        )
        is_a_request = (
            not type(self.body).__name__.lower().endswith("response")
        )

        from etpproto.error import ETPError, MaxSizeExceededError

        # Header encoding
        header_schema = json.loads(mh.avro_schema)
        out_h0 = BytesIO()
        if self.header:
            schemaless_writer(
                out_h0, header_schema, self.header.dict(by_alias=True)
            )

        # Body encoding
        out_body = BytesIO()
        objSchema = json.loads(avro_schema(type(self.body)))
        schemaless_writer(out_body, objSchema, self.body.dict(by_alias=True))

        # Size computation
        headerSize = int(out_h0.getbuffer().nbytes)
        bodySize = int(out_body.getbuffer().nbytes)
        try:
            if (
                max_bytes_per_msg > 0
                and headerSize + bodySize > max_bytes_per_msg
            ):
                # Message exceed the max_bytes_per_msg
                if self.is_plural_msg():
                    msg1 = deepcopy(self)
                    # get the first dict attribute
                    # split it in x factor (as the message is x times too large)
                    cuttable_attrib_name = get_first_dict_attribute_name(
                        msg1.body
                    )

                    values = []
                    if cuttable_attrib_name is not None:  # on a bien un dict
                        values = getattr(msg1.body, cuttable_attrib_name)
                    else:
                        cuttable_attrib_name = get_first_list_attribute_name(
                            msg1.body
                        )
                        values = getattr(msg1.body, cuttable_attrib_name)

                    if len(values) > 1:
                        msg2 = deepcopy(self)
                        msg2.header.message_id = connection.consume_msg_id()

                        # si msg1 etait un vrai premier message on met le suivant au bon correlation id
                        if msg1.header.correlation_id == 0:
                            msg2.header.correlation_id = msg1.header.message_id

                        if is_a_request:
                            msg2.header.correlation_id = correlation_id
                        # else : # rien a changer le correlation_id est deja sur le meme que self car c'est celui de la requete recu

                        d0: Union[list, Dict[Any, Any]] = {}
                        d1: Union[list, Dict[Any, Any]] = {}

                        if isinstance(values, dict):  # on a bien un dict
                            # splitting dict in 2 parts
                            for k, v in values.items():
                                if len(d0) < len(values) / 2:
                                    d0[k] = v
                                else:
                                    d1[k] = v
                        else:
                            d0 = []
                            d1 = []
                            # splitting list in 2 parts
                            for v in values:
                                if len(d0) < len(values) / 2:
                                    d0.append(v)
                                else:
                                    d1.append(v)

                        setattr(msg1.body, cuttable_attrib_name, d0)
                        setattr(msg2.body, cuttable_attrib_name, d1)

                        msg1.add_header_flag(MessageFlags.MULTIPART)
                        msg2.add_header_flag(MessageFlags.MULTIPART)

                        msg1.set_final_msg(False)
                        async for part in msg1.encode_message_generator(
                            max_bytes_per_msg, connection
                        ):
                            yield part
                        async for part in msg2.encode_message_generator(
                            max_bytes_per_msg, connection
                        ):
                            yield part
                    else:
                        if (
                            isinstance(values, dict)
                            and len(values) > 0
                            and self.is_chunkable()
                        ):
                            async for part in _encode_message_generator_chunk(
                                chunkable_msg=self,
                                encoded_msg_size=bodySize,
                                max_bytes_per_msg=max_bytes_per_msg,
                                connection=connection,
                            ):
                                yield part
                        else:
                            raise MaxSizeExceededError()
                else:
                    raise MaxSizeExceededError()
            else:  # Original Message doesn't exceed max_bytes_per_msg
                # self.set_final_msg()
                yield out_h0.getvalue() + out_body.getvalue()
        except ETPError as err:
            msg_err = err.to_etp_message(
                msg_id=connection.consume_msg_id(),
                correlation_id=self.header.correlation_id
                if self.header.correlation_id != 0
                else 0,
            )
            if msg_err is not None:
                msg_err.set_final_msg(True)
                async for part in msg_err.encode_message_generator(
                    -1, connection
                ):
                    yield part
            else:
                raise err

    def is_partial(self) -> bool:
        return isinstance(self.body, bytes)

    def is_final_msg(self) -> bool:
        return self.header.message_flags & MessageFlags.FINALPART != 0

    def is_multipart_msg(self) -> bool:
        return self.header.message_flags & MessageFlags.MULTIPART != 0

    def is_msg_body_compressed(self) -> bool:
        return self.header.message_flags & MessageFlags.COMPRESSED != 0

    def is_asking_acknowledge(self) -> bool:
        return self.header.message_flags & MessageFlags.ACKNOWLEDGE != 0

    def is_including_optional_extension(self) -> bool:
        return (
            self.header.message_flags & MessageFlags.HAS_HEADER_EXTENSION != 0
        )

    def set_final_msg(self, is_final: bool) -> None:
        if is_final:
            self.add_header_flag(MessageFlags.FINALPART)
        else:
            self.remove_header_flag(MessageFlags.FINALPART)

    def add_header_flag(self, msg_flag: MessageFlags) -> None:
        self.header.message_flags = self.header.message_flags | msg_flag

    def remove_header_flag(self, msg_flag: MessageFlags) -> None:
        self.header.message_flags = self.header.message_flags & ~(msg_flag)

    def is_plural_msg(self):
        if self.body:
            return (
                re.fullmatch(r".*s(Response)?$", type(self.body).__name__)
                is not None
            )
        return False

    def is_chunk_msg(self) -> bool:
        return type(self.body).__name__.lower() == "chunk"

    def is_chunkable(self) -> bool:
        return hasattr(self.body, "data_objects")

    def is_chunk_msg_referencer(self):
        """
        Returns true if the message is a "chunkable" one and doesn't contain any data, but a blobId.
        This means that data will be sent after in chunk messages
        """
        if not self.is_chunk_msg() and self.is_chunkable():
            if isinstance(self.body.data_objects, list):
                for do in self.body.data_objects:
                    if not (
                        do.blob_id is not None
                        and (do.data is None or do.data == "")
                    ):
                        return False
                return True
            else:
                for k, do in self.body.data_objects.items():
                    if not (
                        do.blob_id is not None
                        and (do.data is None or len(do.data) == 0)
                    ):
                        return False
                return True

    @classmethod
    def reassemble_chunk(
        cls, multipart_msg: List[Message]
    ) -> Optional[Message]:
        referencer: Optional[Message] = None

        # on rassemble tout dans un seul msg (concat de toutes les map)
        for msg in multipart_msg:
            if msg.is_chunk_msg_referencer():
                if referencer is None:
                    referencer = msg
                else:
                    # assert referencer.body is not None
                    referencer.body.data_objects = (  # type: ignore[attr-defined]
                        msg.body.data_objects  # type: ignore[attr-defined]
                        if referencer.body.data_objects is None  # type: ignore[attr-defined]
                        else concat(
                            referencer.body.data_objects,  # type: ignore[attr-defined]
                            msg.body.data_objects,  # type: ignore[attr-defined]
                        )
                    )

        result = None

        assert referencer is not None
        do_collection = referencer.body.data_objects  # type: ignore[attr-defined]

        # todo : faire un sort ?
        if referencer is not None and do_collection is not None:

            def inner_operate_data_object(data_object: DataObject):
                for msg in multipart_msg:
                    if (
                        msg.is_chunk_msg()
                        and msg.body.blob_id == data_object.blob_id  # type: ignore[attr-defined]
                    ):
                        data_object.data = (
                            msg.body.data  # type: ignore[attr-defined]
                            if data_object.data is None
                            or len(data_object.data) == 0
                            else data_object.data + msg.body.data  # type: ignore[attr-defined]
                        )
                    # TODO : on ne test pas le "is_final" du dernier chunk mais peut importe

                if data_object.data is not None and len(data_object.data) > 0:
                    data_object.blob_id = (
                        None  # pas de blob_id ET data en meme temps
                    )
                return data_object

            if isinstance(do_collection, list):
                for do in do_collection:
                    inner_operate_data_object(do)
            elif isinstance(do_collection, dict):
                for k, do in do_collection.items():
                    inner_operate_data_object(do)
            else:
                logging.error(
                    f"#etpproto.message@reassemble_chunk : not supported chunckable message data_objects type : {type(do_collection)}"
                )
            result = referencer

        return result

    @classmethod
    def decode_binary_message(
        cls, binary: bytes, dict_map_pro_to_class: ProtocolDict
    ) -> Optional[Message]:
        fo = BytesIO(binary)
        recMH = schemaless_reader(
            fo,
            json.loads(mh.avro_schema),
            return_record_name=True,
            return_record_name_override=True,
        )
        posAfterHeaderRead = fo.tell()

        if recMH["protocol"] >= 0:
            try:
                object_class = dict_map_pro_to_class[str(recMH["protocol"])][
                    str(recMH["messageType"])
                ]

                # logging.debug("##> len : {len(binary)} posAfterHeaderRead {posAfterHeaderRead} fotell {fo.tell()}")

                object_res = schemaless_reader(
                    fo,
                    json.loads(avro_schema(object_class)),
                    return_record_name=True,
                    return_record_name_override=True,
                )

                logging.debug(f"HEADER {recMH}")

                logging.debug(
                    f"classmethod decode_binary_message {object_res}"
                )
                logging.debug(f" ==> object_class {object_class}")

                return Message(
                    mh.MessageHeader.parse_obj(recMH),
                    object_class.parse_obj(object_res),
                )
            except Exception as e:
                logging.error(f"{e}")
                # error, now we try to read it as an error, because error has now the protocol of the message send by the client
                # try:
                object_class = dict_map_pro_to_class["0"][
                    str(recMH["messageType"])
                ]

                logging.debug(f" ==> object_class {object_class}")

                object_res = schemaless_reader(
                    fo,
                    json.loads(avro_schema(object_class)),
                    return_record_name=True,
                    return_record_name_override=True,
                )
                return Message(
                    mh.MessageHeader.parse_obj(recMH),
                    object_class.parse_obj(object_res),
                )
                # except Exception:
                #     traceback.print_exc()
                #     logging.error("### ERR : in decode_binary_message")
                #     logging.error(f"{e}")
                #     pass

        # If the message has not been read, it's should be a partial message
        fo.seek(posAfterHeaderRead)
        return None  # Message(mh.MessageHeader.parse_obj(recMH), fo.read())

    @classmethod
    def get_object_message(
        cls,
        etp_object: ETPModel,
        msg_id: int = -1,
        has_header: bool = True,
        correlation_id: int = 0,
        message_flags: int = 0,
    ) -> Optional[Message]:
        if etp_object:
            logging.debug(f"get_object_message {etp_object}")
            logging.debug(f"get_object_message {type(etp_object)}")

            objSchema = json.loads(avro_schema(type(etp_object)))

            header = None
            if has_header:
                header = mh.MessageHeader(
                    protocol=int(objSchema["protocol"]),
                    messageType=int(objSchema["messageType"]),
                    correlationId=correlation_id,
                    messageId=msg_id,
                    messageFlags=message_flags,
                )
                return Message(header, etp_object)
        return None


def header_has_flag(header: mh.MessageHeader, flag: MessageFlags):
    return header.message_flags & flag != 0


def decode_binary_message(
    binary: bytes, dict_map_pro_to_class: ProtocolDict
) -> Tuple[mh.MessageHeader, ETPModel]:
    fo = BytesIO(binary)
    recMH = schemaless_reader(
        fo,
        json.loads(mh.avro_schema),
        return_record_name=True,
        return_record_name_override=True,
    )
    object_class = dict_map_pro_to_class[str(recMH["protocol"])][
        str(recMH["messageType"])
    ]
    object_res = schemaless_reader(
        fo,
        json.loads(avro_schema(object_class)),
        return_record_name=True,
        return_record_name_override=True,
    )

    logging.debug(f"decode_binary_message {object_res}")

    return (
        mh.MessageHeader.parse_obj(recMH),
        object_class.parse_obj(object_res),
    )


# When calling thins function we are supposed to have only one entry in the data_objects map/list
async def _encode_message_generator_chunk(
    chunkable_msg: Message,
    encoded_msg_size: int,
    max_bytes_per_msg: int,
    connection,
) -> AsyncGenerator[bytes, None]:
    from etpproto.error import InternalError

    secure_size = 50  # TODO : ameliorer pour que le chunk fasse vraiment la taille max d'un message (il faudrait connaitre la taille de ce qui n'est pas binaire dans le chunk message)
    size_of_chunks = max_bytes_per_msg - secure_size

    data_objs = chunkable_msg.body.data_objects  # type: ignore[attr-defined]
    msg_was_final = chunkable_msg.is_final_msg()

    correlation_id = (
        chunkable_msg.header.correlation_id
        if chunkable_msg.header.correlation_id != 0
        else chunkable_msg.header.message_id
    )

    # else : # rien a changer le correlation_id est deja sur le meme que self car c'est celui de la requete recu

    if data_objs:  # si on a une list/Map de dataObjects
        nb_chunks = ceil(
            encoded_msg_size / size_of_chunks
        )  # substract 50 for header part (header takes 5?) and non binary part of the chunk message
        logging.debug(
            f"Size of chunks : {size_of_chunks} msgS {encoded_msg_size}"
        )
        # get the chunks class
        chunk_class = get_class_from_protocol_and_name(
            str(chunkable_msg.header.protocol),
            "chunk",
            connection.generic_transition_table,
        )

        if chunk_class is not None:
            lst_chunks = []  # all chunks of all dataObjects

            # for blob_id see 3.7.3.2 of the documentation :
            # blob_id is assign to one entire DataObject and is refered in all chunck of this dataObject

            if isinstance(data_objs, dict):
                for k, do in data_objs.items():
                    data = do.data
                    blob_id = Uuid(pyUUID.uuid4().bytes)

                    # nb_chunks = ceil(len(data)/size_of_chunks)
                    for c_i in range(nb_chunks):
                        # create chunk msg
                        chunk_msg = chunk_class(
                            blob_id=blob_id,
                            data=data[
                                c_i * size_of_chunks : c_i * size_of_chunks
                                + size_of_chunks
                            ],
                            final=False,
                        )
                        lst_chunks.append(chunk_msg)

                    if len(lst_chunks) > 0:
                        lst_chunks[-1].final = True
                    do.blob_id = blob_id
                    do.data = b""  # removing the data from the message when blob_id is populated
            elif isinstance(data_objs, list):
                # on parcourt la liste des DataObjects
                # on cree plusieurs chunk pour chacuns
                for do in data_objs:
                    data = do.data
                    blob_id = Uuid(pyUUID.uuid4().bytes)

                    # nb_chunks = ceil(len(data)/size_of_chunks)
                    logging.debug(f"Nb chunks : {nb_chunks}")
                    for c_i in range(nb_chunks):
                        # create chunk msg
                        chunk_msg = chunk_class(
                            blob_id=blob_id,
                            data=data[
                                c_i * size_of_chunks : c_i * size_of_chunks
                                + size_of_chunks
                            ],
                            final=False,
                        )
                        lst_chunks.append(chunk_msg)

                    if len(lst_chunks) > 0:
                        lst_chunks[-1].final = True
                    do.blob_id = blob_id
                    do.data = b""  # removing the data from the message when blob_id is populated

            # chunkable_msg.data_objects = data_objs  # useless ?
            # send the message
            chunkable_msg.set_final_msg(False)
            chunkable_msg.add_header_flag(MessageFlags.MULTIPART)
            async for part in chunkable_msg.encode_message_generator(
                max_bytes_per_msg, connection
            ):
                yield part

            # send chunks
            for chunk in lst_chunks[:-1]:
                current_chunk_msg = Message.get_object_message(
                    etp_object=chunk,
                    msg_id=connection.consume_msg_id(),
                    has_header=True,
                    correlation_id=correlation_id,
                    message_flags=MessageFlags.MULTIPART,
                )
                if current_chunk_msg is not None:
                    async for part in current_chunk_msg.encode_message_generator(
                        max_bytes_per_msg, connection
                    ):
                        yield part
                else:
                    raise InternalError(
                        "@Message : error during chunk creation "
                        + str(chunkable_msg.header.protocol)
                    )

            # denier chunk, mettre le final a True
            final_chunk_msg = Message.get_object_message(
                etp_object=lst_chunks[-1],
                msg_id=connection.consume_msg_id(),
                has_header=True,
                correlation_id=correlation_id,
                message_flags=MessageFlags.MULTIPART_AND_FINALPART
                if msg_was_final
                else MessageFlags.MULTIPART,
            )
            if final_chunk_msg is not None:
                async for part in final_chunk_msg.encode_message_generator(
                    max_bytes_per_msg, connection
                ):
                    yield part

                # TODO : potentielle erreur de MultipartCancelledError si le nb de reponse depasse le nombre max
            else:
                raise InternalError(
                    "@Message : error during chunk creation "
                    + str(chunkable_msg.header.protocol)
                )

        else:
            raise InternalError(
                "@Message : No chunck class found for protocol "
                + str(chunkable_msg.header.protocol)
            )
    else:
        raise InternalError(
            "@Message : No data_object found "
            + str(chunkable_msg.header.protocol)
        )
    # raise une erreur ?
