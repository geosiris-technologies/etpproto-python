import asyncio
import json
from copy import deepcopy
from io import BytesIO
from math import ceil

import etptypes.energistics.etp.v12.datatypes.message_header as mh
import etptypes.energistics.etp.v12.protocol.core.open_session as op
import etptypes.energistics.etp.v12.protocol.discovery.get_resources as gr
import etptypes.energistics.etp.v12.protocol.store.chunk as store_chunk
import pytest
from etptypes.energistics.etp.v12.datatypes.data_value import DataValue
from etptypes.energistics.etp.v12.datatypes.object.active_status_kind import (
    ActiveStatusKind,
)
from etptypes.energistics.etp.v12.datatypes.object.context_info import (
    ContextInfo,
)
from etptypes.energistics.etp.v12.datatypes.object.context_scope_kind import (
    ContextScopeKind,
)
from etptypes.energistics.etp.v12.datatypes.object.data_object import (
    DataObject,
)
from etptypes.energistics.etp.v12.datatypes.object.relationship_kind import (
    RelationshipKind,
)
from etptypes.energistics.etp.v12.datatypes.object.resource import Resource
from etptypes.energistics.etp.v12.protocol.core.protocol_exception import (
    ProtocolException,
)
from etptypes.energistics.etp.v12.protocol.discovery.get_resources_response import (
    GetResourcesResponse,
)
from etptypes.energistics.etp.v12.protocol.store.get_data_objects_response import (
    GetDataObjectsResponse,
)
from fastavro import schemaless_reader, schemaless_writer

from etpproto.error import (
    ETPError,
    InvalidMessageError,
    UnsupportedProtocolError,
    MaxSizeExceededError,
)
from etpproto.messages import Message, MessageFlags

# try:
from .server_protocol_example import *

# except:
# from server_protocol_example import *


etp_version = Version(major=1, minor=2, revision=0, patch=0)
local_protocols = [
    SupportedProtocol(
        protocol=0,
        protocol_version=etp_version,
        role="server",
        protocol_capabilities={},
    ),
    SupportedProtocol(
        protocol=3,
        protocol_version=etp_version,
        role="store",
        protocol_capabilities={},
    ),
]

supported_objects = [
    SupportedDataObject(
        qualified_type="resqml20",
        data_object_capabilities={},
    )
]

requestSession_obj = RequestSession(
    application_name="WebStudio",
    application_version="1.2",
    client_instance_id=uuid.uuid4(),
    requested_protocols=local_protocols,
    supported_data_objects=supported_objects,
    supported_compression=["string"],
    supported_formats=["xml"],
    current_date_time=int(datetime.utcnow().timestamp()),
    endpoint_capabilities={},
    earliest_retained_change_time=int(datetime.utcnow().timestamp()),
)
requestSession_msg = Message.get_object_message(requestSession_obj, msg_id=1)

getResources_msg = Message.get_object_message(
    gr.GetResources(
        context=ContextInfo(
            uri="eml:///",
            depth=1,
            data_object_types=[],
            navigable_edges=RelationshipKind.PRIMARY,
        ),
        scope=ContextScopeKind.SELF,
        count_objects=False,
        store_last_write_filter=None,
        active_status_filter=ActiveStatusKind.INACTIVE,
        include_edges=False,
    ),
    msg_id=2,
)
closeSession_msg = Message.get_object_message(
    CloseSession(reason="On a fini et c'est tout!"), msg_id=3
)

resqml_obj_37166c33_3ebb_40ae_9bc6_1ab9693def60 = bytes(
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><resqml2:HorizonInterpretation xmlns:resqml2="http://www.energistics.org/energyml/data/resqmlv2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" schemaVersion="2.0" uuid="37166c33-3ebb-40ae-9bc6-1ab9693def60" xsi:type="resqml2:obj_HorizonInterpretation"> <eml:Citation xmlns:eml="http://www.energistics.org/energyml/data/commonv2" xsi:type="eml:Citation">        <eml:Title xsi:type="eml:DescriptionString">SnS</eml:Title>     <eml:Originator xsi:type="eml:NameString">Unknown</eml:Originator>      <eml:Creation xmlns:xsd="http://www.w3.org/2001/XMLSchema" xsi:type="xsd:dateTime">2014-12-18T10:47:47Z</eml:Creation>      <eml:Format xsi:type="eml:DescriptionString">SISMAGE</eml:Format>       <eml:Editor xsi:type="eml:NameString">Unknown</eml:Editor>  </eml:Citation> <resqml2:Domain xsi:type="resqml2:Domain">depth</resqml2:Domain>    <resqml2:InterpretedFeature xmlns:eml="http://www.energistics.org/energyml/data/commonv2" xsi:type="eml:DataObjectReference">       <eml:ContentType xmlns:xsd="http://www.w3.org/2001/XMLSchema" xsi:type="xsd:string">application/x-resqml+xml;version=2.0;type=obj_GeneticBoundaryFeature</eml:ContentType>      <eml:Title xsi:type="eml:DescriptionString">TopN2-2</eml:Title>     <eml:UUID xsi:type="eml:UuidString">85cad705-0228-4f31-905c-a22077316479</eml:UUID>     </resqml2:InterpretedFeature></resqml2:HorizonInterpretation>',
    encoding="utf-8",
)
resqml_obj_260690d5_adc3_4f2c_b53e_2ff16345f52f = bytes(
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><resqml2:HorizonInterpretation xmlns:resqml2="http://www.energistics.org/energyml/data/resqmlv2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" schemaVersion="2.0" uuid="260690d5-adc3-4f2c-b53e-2ff16345f52f" xsi:type="resqml2:obj_HorizonInterpretation">   <eml:Citation xmlns:eml="http://www.energistics.org/energyml/data/commonv2" xsi:type="eml:Citation">        <eml:Title xsi:type="eml:DescriptionString">SnS</eml:Title>     <eml:Originator xsi:type="eml:NameString">Unknown</eml:Originator>      <eml:Creation xmlns:xsd="http://www.w3.org/2001/XMLSchema" xsi:type="xsd:dateTime">2014-12-18T10:47:47Z</eml:Creation>      <eml:Format xsi:type="eml:DescriptionString">SISMAGE</eml:Format>       <eml:Editor xsi:type="eml:NameString">Unknown</eml:Editor>  </eml:Citation> <resqml2:Domain xsi:type="resqml2:Domain">depth</resqml2:Domain>    <resqml2:InterpretedFeature xmlns:eml="http://www.energistics.org/energyml/data/commonv2" xsi:type="eml:DataObjectReference">       <eml:ContentType xmlns:xsd="http://www.w3.org/2001/XMLSchema" xsi:type="xsd:string">application/x-resqml+xml;version=2.0;type=obj_GeneticBoundaryFeature</eml:ContentType>      <eml:Title xsi:type="eml:DescriptionString">TopEtive</eml:Title>        <eml:UUID xsi:type="eml:UuidString">f6e3f59f-8ca3-4497-838d-5db3015f2428</eml:UUID>     </resqml2:InterpretedFeature></resqml2:HorizonInterpretation>',
    encoding="utf-8",
)

dataObjectResponse_msg = Message.get_object_message(
    GetDataObjectsResponse(
        data_objects={
            "0": DataObject(
                resource=Resource(
                    uri="37166c33-3ebb-40ae-9bc6-1ab9693def60",
                    data_object_type="HorizonInterpretation",
                    name="TopN2-2",
                    last_changed=0,
                    store_created=0,
                    active_status=ActiveStatusKind.INACTIVE,
                    store_last_write=0,
                    alternate_uris=[],
                    custom_data={},
                ),
                data=resqml_obj_37166c33_3ebb_40ae_9bc6_1ab9693def60,
            ),
            "1": DataObject(
                resource=Resource(
                    uri="260690d5-adc3-4f2c-b53e-2ff16345f52f",
                    data_object_type="obj_GeneticBoundaryFeature",
                    name="TopEtive",
                    last_changed=0,
                    store_created=0,
                    active_status=ActiveStatusKind.INACTIVE,
                    store_last_write=0,
                    alternate_uris=[],
                    custom_data={},
                ),
                data=resqml_obj_260690d5_adc3_4f2c_b53e_2ff16345f52f,
            ),
        }
    ),
    msg_id=2,
)
ressourceResponse_msg = Message.get_object_message(
    GetResourcesResponse(
        resources=[
            Resource(
                uri="eml:///resqml22.FaultInterpretation(2bf956eb-b527-43f8-94f5-6e3972b84d65)",
                data_object_type="",
                name="test",
                source_count=0,
                target_count=0,
                storeCreated=0,
                active_status=ActiveStatusKind.ACTIVE,
                last_changed=0,
                store_last_write=0,
                alternate_uris=[],
                custom_data={},
            )
        ]
    ),
    msg_id=2,
)
for i in range(20):
    ressourceResponse_msg.body.resources.append(
        ressourceResponse_msg.body.resources[0]
    )


@pytest.mark.asyncio
async def test_msg_finality():
    assert not requestSession_msg.is_final_msg()
    requestSession_msg.set_final_msg(True)
    assert requestSession_msg.is_final_msg()
    requestSession_msg.set_final_msg(False)
    assert not requestSession_msg.is_final_msg()


@pytest.mark.asyncio
async def test_msg_multiparity():
    assert not requestSession_msg.is_multipart_msg()
    requestSession_msg.add_header_flag(MessageFlags.MULTIPART)
    assert requestSession_msg.is_multipart_msg()
    requestSession_msg.remove_header_flag(MessageFlags.MULTIPART)
    assert not requestSession_msg.is_multipart_msg()


@pytest.mark.asyncio
async def test_msg_simple_msg_exceed_size():
    size_limit = 20
    connection_client = ETPConnection(connection_type=ConnectionType.CLIENT)

    async for result in requestSession_msg.encode_message_generator(
        size_limit, connection_client
    ):
        decoded = Message.decode_binary_message(
            result, ETPConnection.generic_transition_table
        )
        assert isinstance(decoded.body, ProtocolException)
        assert decoded.body.error.code == MaxSizeExceededError.code


@pytest.mark.asyncio
async def test_msg_multipart_map_split_in_2_parts():
    assert len(dataObjectResponse_msg.body.data_objects) == 2
    size_limit = 2000
    connection_client = ETPConnection(connection_type=ConnectionType.CLIENT)
    dataObjectResponse_msg.set_final_msg(True)

    decoded_partial_msg_list = []
    async for part in dataObjectResponse_msg.encode_message_generator(
        size_limit, connection_client
    ):
        assert len(part) <= size_limit
        decoded_partial_msg_list.append(
            Message.decode_binary_message(
                part, ETPConnection.generic_transition_table
            )
        )

    assert len(decoded_partial_msg_list) == 2
    # decoded = Message.decode_partial_message_list(
    #     decoded_partial_msg_list, ETPConnection.generic_transition_table
    # )
    for i, decoded_k in enumerate(decoded_partial_msg_list):
        assert isinstance(decoded_k.body, GetDataObjectsResponse)
        # print(">---------->", list(decoded_k.body.data_objects.values())[0].resource.uri)
        assert len(decoded_k.body.data_objects) == 1

        # test if only last message has fin bit set
        if i < len(decoded_partial_msg_list) - 1:
            assert not decoded_k.is_final_msg()
        else:
            assert decoded_k.is_final_msg()


@pytest.mark.asyncio
async def test_msg_multipart_map_split_in_2_parts_and_chunks():
    nb_data_objects = len(dataObjectResponse_msg.body.data_objects)
    assert len(dataObjectResponse_msg.body.data_objects) == 2
    size_limit = 500

    nb_chunks = ceil(
        len(resqml_obj_37166c33_3ebb_40ae_9bc6_1ab9693def60)
        / (size_limit - 50)
    )  # 50 is the size limit security (see message.py)

    connection_client = ETPConnection(connection_type=ConnectionType.CLIENT)
    dataObjectResponse_msg.set_final_msg(True)

    decoded_partial_msg_list = []
    async for part in dataObjectResponse_msg.encode_message_generator(
        size_limit, connection_client
    ):
        assert len(part) <= size_limit
        decoded_partial_msg_list.append(
            Message.decode_binary_message(
                part, ETPConnection.generic_transition_table
            )
        )

    assert len(decoded_partial_msg_list) == nb_data_objects * (
        1 + nb_chunks
    )  # 1 GetDataObjectResponse and 4 chunks for each dataObject, so 10

    chunk_total_size = 0

    for i, decoded_k in enumerate(decoded_partial_msg_list):
        # print("\n\n>>> ", i, ")", decoded_k.header,  decoded_k.body, "  --- ", type(decoded_k.body), "\n\n")
        if i == 0:
            assert decoded_k.header.correlation_id == 0
        else:
            assert (
                decoded_k.header.correlation_id
                == decoded_partial_msg_list[0].header.message_id
            )

        if i == 0 or i == int(len(decoded_partial_msg_list) / 2):
            assert isinstance(decoded_k.body, GetDataObjectsResponse)
            # print(">>", list(decoded_k.body.data_objects.values())[0].resource.uri)
            assert len(decoded_k.body.data_objects) == 1
            # data must be empty if have chunks messages
            assert len(list(decoded_k.body.data_objects.values())[0].data) == 0

            assert decoded_k.is_chunk_msg_referencer()
        else:
            assert isinstance(decoded_k.body, store_chunk.Chunk)
            chunk_total_size += len(decoded_k.body.data)
            assert decoded_k.is_chunk_msg()

        # test if only last message has fin bit set
        if i < len(decoded_partial_msg_list) - 1:
            assert not decoded_k.is_final_msg()
        else:
            assert decoded_k.is_final_msg()

        assert decoded_k.is_multipart_msg()

    # print(type(resqml_obj_37166c33_3ebb_40ae_9bc6_1ab9693def60), "\n", resqml_obj_37166c33_3ebb_40ae_9bc6_1ab9693def60)
    assert chunk_total_size == len(
        resqml_obj_37166c33_3ebb_40ae_9bc6_1ab9693def60.decode("utf-8")
    ) + len(resqml_obj_260690d5_adc3_4f2c_b53e_2ff16345f52f.decode("utf-8"))

    dataObjectResponse_msg.set_final_msg(False)


@pytest.mark.asyncio
async def test_msg_multipart_chunks_reassembled_in_connection():
    nb_data_objects = len(dataObjectResponse_msg.body.data_objects)
    size_limit = 500
    connection_client = ETPConnection(connection_type=ConnectionType.CLIENT)
    dataObjectResponse_msg.set_final_msg(True)

    nb_chunks = ceil(
        len(resqml_obj_37166c33_3ebb_40ae_9bc6_1ab9693def60)
        / (size_limit - 50)
    )  # 50 is the size limit security (see message.py)

    decoded_partial_msg_list = []
    async for part in dataObjectResponse_msg.encode_message_generator(
        size_limit, connection_client
    ):
        assert len(part) <= size_limit
        decoded_partial_msg_list.append(
            Message.decode_binary_message(
                part, ETPConnection.generic_transition_table
            )
        )

    assert len(decoded_partial_msg_list) == nb_data_objects * (1 + nb_chunks)

    for msg in decoded_partial_msg_list:
        assert msg.is_chunk_msg() or msg.is_chunk_msg_referencer()

    reassemble_result = Message.reassemble_chunk(decoded_partial_msg_list)
    assert type(reassemble_result) == type(dataObjectResponse_msg)
    assert len(reassemble_result.body.data_objects) == 2
    for idx, do in reassemble_result.body.data_objects.items():
        assert len(do.data) > size_limit
        assert dataObjectResponse_msg.body.data_objects[idx].data == do.data


@pytest.mark.asyncio
async def test_msg_multipart_chunks_reassembled_in_connection_get_ressources():
    size_limit = 500
    connection_client = ETPConnection(connection_type=ConnectionType.CLIENT)
    ressourceResponse_msg.set_final_msg(True)

    decoded_partial_msg_list = []
    async for part in ressourceResponse_msg.encode_message_generator(
        size_limit, connection_client
    ):
        assert len(part) <= size_limit
        decoded_partial_msg_list.append(
            Message.decode_binary_message(
                part, ETPConnection.generic_transition_table
            )
        )


# if __name__ == "__main__":
#     asyncio.run(test_connection_multipart_msg_one_part_answer_generator())
