# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

import pytest
from etptypes.energistics.etp.v12.datatypes.object.context_info import (
    ContextInfo,
)
from etptypes.energistics.etp.v12.datatypes.object.context_scope_kind import (
    ContextScopeKind,
)
from etptypes.energistics.etp.v12.datatypes.object.relationship_kind import (
    RelationshipKind,
)
from etptypes.energistics.etp.v12.protocol.core.acknowledge import Acknowledge
from etptypes.energistics.etp.v12.protocol.core.protocol_exception import (
    ProtocolException,
)

from etpproto.error import (
    InvalidMessageError,
    InvalidStateError,
)

try:
    from .server_protocol_example import *
except Exception:
    from server_protocol_example import *

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
        data_object_capabilities={},  # DataObjectCapabilityKind.SUPPORTS_GET: DataValue(item=False)
    )
]


requestSession_msg = Message.get_object_message(
    RequestSession(
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
    ),
    msg_id=2,  # to test the correlation id
)

requestSession_msg_ask_acknowledge = Message.get_object_message(
    RequestSession(
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
    ),
    msg_id=1,
    message_flags=0x10,
)


supportedProtocolList = ETPConnection.get_supported_protocol_list()
openSession_msg = Message.get_object_message(
    OpenSession(
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
        session_id=uuid.uuid4(),
        current_date_time=int(datetime.utcnow().timestamp()),
        endpoint_capabilities={},
        earliest_retained_change_time=int(datetime.utcnow().timestamp()),
    ),
    msg_id=1,
)

getResources_msg = Message.get_object_message(
    GetResources(
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
    # correlation_id = 17
)


@pytest.mark.asyncio
async def test_connection_state_init() -> None:
    connection = ETPConnection()
    assert not connection.is_connected


@pytest.mark.asyncio
async def test_connection_server_first_msg_id() -> None:
    connection = ETPConnection(connection_type=ConnectionType.SERVER)
    assert connection.message_id == 1


@pytest.mark.asyncio
async def test_connection_client_first_msg_id() -> None:
    connection = ETPConnection(connection_type=ConnectionType.CLIENT)
    assert connection.message_id == 2


@pytest.mark.asyncio
async def test_connection_state_open_session_client() -> None:
    connection = ETPConnection(connection_type=ConnectionType.CLIENT)
    print(openSession_msg.body.dict(by_alias=True))
    async for m in connection._handle_message_generator(openSession_msg):
        # print(m)
        assert m is None
        pass

    assert connection.is_connected


@pytest.mark.asyncio
async def test_connection_state_open_session_server() -> None:
    connection = ETPConnection(connection_type=ConnectionType.SERVER)
    print(openSession_msg.body.dict(by_alias=True))
    async for m in connection._handle_message_generator(openSession_msg):
        pass

    assert not connection.is_connected


@pytest.mark.asyncio
async def test_connection_state_close_session() -> None:
    connection = ETPConnection()
    async for m in connection._handle_message_generator(requestSession_msg):
        pass
    async for m in connection._handle_message_generator(closeSession_msg):
        pass

    assert not connection.is_connected


@pytest.mark.asyncio
async def test_connection_state_2_distincts() -> None:
    connection_a = ETPConnection()
    connection_b = ETPConnection()

    assert not connection_a.is_connected
    assert not connection_b.is_connected

    async for m in connection_a._handle_message_generator(requestSession_msg):
        pass

    assert connection_a.is_connected
    assert not connection_b.is_connected

    async for m in connection_a._handle_message_generator(closeSession_msg):
        pass

    assert not connection_a.is_connected
    assert not connection_b.is_connected


@pytest.mark.asyncio
async def test_connection_state_request_session_as_bytes() -> None:
    connection = ETPConnection()

    async for m in connection.handle_bytes_generator(
        requestSession_msg.encode_message()
    ):
        pass

    assert connection.is_connected


@pytest.mark.asyncio
async def test_connection_state_close_session_as_bytes() -> None:
    connection = ETPConnection()

    async for m in connection.handle_bytes_generator(
        requestSession_msg.encode_message()
    ):
        pass

    async for m in connection.handle_bytes_generator(
        closeSession_msg.encode_message()
    ):
        pass

    assert not connection.is_connected


@pytest.mark.asyncio
async def test_connection_requestSession_answer() -> None:
    connection = ETPConnection()

    answer = []
    async for m in connection._handle_message_generator(requestSession_msg):
        answer.append(m)

    assert len(answer) == 1
    assert isinstance(answer[0].body, OpenSession)


@pytest.mark.asyncio
async def test_connection_requestSession_answer_as_bytes() -> None:
    connection = ETPConnection()

    answer = []
    async for m in connection.handle_bytes_generator(
        requestSession_msg.encode_message()
    ):
        answer.append(m)

    assert len(answer) == 1

    answer_msg = Message.decode_binary_message(
        answer[0], ETPConnection.generic_transition_table
    )
    assert isinstance(answer_msg.body, OpenSession)


@pytest.mark.asyncio
async def test_connection_requestSession_answer_as_bytes_generator() -> None:
    connection = ETPConnection()

    answer = []
    async for m in (
        connection.handle_bytes_generator(requestSession_msg.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 1
    assert answer[0].is_final_msg()
    assert isinstance(answer[0].body, OpenSession)


@pytest.mark.asyncio
async def test_connection_closeSession_answer() -> None:
    connection = ETPConnection()

    async for m in connection._handle_message_generator(requestSession_msg):
        pass

    answer = []
    async for m in connection._handle_message_generator(closeSession_msg):
        answer.append(m)

    assert len(answer) == 0
    assert not connection.is_connected


@pytest.mark.asyncio
async def test_connection_closeSession_answer_as_bytes_generator() -> None:
    connection = ETPConnection()

    async for m in connection._handle_message_generator(requestSession_msg):
        pass

    answer = []
    async for m in (
        connection.handle_bytes_generator(closeSession_msg.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    assert len(answer) == 0
    assert not connection.is_connected


@pytest.mark.asyncio
async def test_send_msg_without_connection_generator() -> None:
    connection = ETPConnection()

    answer = []
    async for m in (
        connection.handle_bytes_generator(getResources_msg.encode_message())
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )

    print("### => ", type(answer[0].body))

    assert len(answer) == 1
    assert isinstance(answer[0].body, ProtocolException)
    assert answer[0].body.error.code == InvalidStateError.code


@pytest.mark.asyncio
async def test_connection_first_msg_attributes() -> None:
    connection = ETPConnection()

    answer = []
    async for m in connection._handle_message_generator(requestSession_msg):
        answer.append(m)

    print(answer[0].header.message_flags)
    assert len(answer) == 1
    assert answer[0].header.message_id == 1
    assert (
        answer[0].header.correlation_id == requestSession_msg.header.message_id
    )
    assert answer[0].is_final_msg()


@pytest.mark.asyncio
async def test_connection_requestSession_answer_as_bytes_generator_acknowledge() -> None:
    connection = ETPConnection()
    assert requestSession_msg_ask_acknowledge.is_asking_acknowledge()

    answer = []
    async for m in (
        connection.handle_bytes_generator(
            requestSession_msg_ask_acknowledge.encode_message()
        )
    ):
        answer.append(
            Message.decode_binary_message(
                m, ETPConnection.generic_transition_table
            )
        )
        print("m ==> ", answer[len(answer) - 1])

    assert len(answer) == 2
    assert answer[1].is_final_msg()
    assert isinstance(answer[0].body, Acknowledge)
    assert isinstance(answer[1].body, OpenSession)


if __name__ == "__main__":
    # asyncio.run(test_send_msg_without_connection_generator())
    # asyncio.run(test_connection_requestSession_answer_as_bytes_generator())

    from fastavro import schemaless_reader, schemaless_writer
    from etptypes import avro_schema
    from io import BytesIO
    import json

    msg = InvalidMessageError().to_etp_message(msg_id=1)
    msg_body_class = type(msg.body)

    out_body = BytesIO()
    print(msg_body_class)
    print(avro_schema(msg_body_class))
    objSchema = json.loads(avro_schema(msg_body_class))
    schemaless_writer(out_body, objSchema, msg.body.dict(by_alias=True))

    out_body.seek(0)
    print("\n")
    res = schemaless_reader(out_body, objSchema, return_record_name=True)

    print(res)
    print(msg_body_class.parse_obj(res))

    ########################
    # from etptypes.energistics.etp.v12.datatypes.data_value import DataValue
    # from etptypes.energistics.etp.v12.datatypes.array_of_float import ArrayOfFloat

    # msg = DataValue(item=ArrayOfFloat(values=[1.0, 2.3]))
    # msg_body_class = type(msg)

    # out_body = BytesIO()
    # print(msg_body_class)
    # print(avro_schema(msg_body_class))
    # objSchema = json.loads(avro_schema(msg_body_class))
    # schemaless_writer(out_body, objSchema, msg.dict(by_alias=True))

    # out_body.seek(0)
    # print("\n##")
    # res = schemaless_reader(out_body, objSchema)

    # print(res)
    # print("==>", msg_body_class.parse_obj(res))

# test_connection_first_msg_attributes()
