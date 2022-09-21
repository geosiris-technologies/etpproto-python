import json
import os
from datetime import datetime

import etptypes.energistics.etp.v12.datatypes.message_header as mh
import etptypes.energistics.etp.v12.protocol.core.open_session as op
import etptypes.energistics.etp.v12.protocol.core.request_session as rs
import etptypes.energistics.etp.v12.protocol.discovery.get_resources as gr
import pytest
from fastavro import reader, schemaless_reader, schemaless_writer, writer


def test_unserialAvroRequestSession() -> None:
    recordsHeader = [
        {
            "protocol": 0,
            "messageType": 1,
            "correlationId": 0,
            "messageId": 1,
            "messageFlags": 0,
        }
    ]
    recordsReqSess = [
        {
            "applicationName": "WebStudio",
            "applicationVersion": "1.2",
            "clientInstanceId": b"360559be-0634-47",
            "requestedProtocols": [
                {
                    "protocol": 0,
                    "protocolVersion": {
                        "major": 1,
                        "minor": 2,
                        "revision": 0,
                        "patch": 0,
                    },
                    "role": "server",
                    "protocolCapabilities": {},
                },
                {
                    "protocol": 3,
                    "protocolVersion": {
                        "major": 1,
                        "minor": 2,
                        "revision": 0,
                        "patch": 0,
                    },
                    "role": "store",
                    "protocolCapabilities": {},
                },
            ],
            "supportedDataObjects": [
                {
                    "qualifiedType": "resqml20",
                    "dataObjectCapabilities": {},
                }
            ],
            "supportedCompression": ["string"],
            "supportedFormats": ["xml"],
            "currentDateTime": 1603705108671,
            "endpointCapabilities": {},
            "earliestRetainedChangeTime": 1603705108671,
        }
    ]
    recordsOpenSess = {
        "applicationName": "etpproto",
        "applicationVersion": "1.0",
        "serverInstanceId": b"360559be-0634-47",
        "supportedProtocols": [
            {
                "protocol": 0,
                "protocolVersion": {
                    "major": 1,
                    "minor": 2,
                    "revision": 0,
                    "patch": 0,
                },
                "role": "server",
                "protocolCapabilities": {},
            },
            {
                "protocol": 3,
                "protocolVersion": {
                    "major": 1,
                    "minor": 2,
                    "revision": 0,
                    "patch": 0,
                },
                "role": "store",
                "protocolCapabilities": {},
            },
        ],
        "supportedDataObjects": [
            {
                "qualifiedType": "resqml20",
                "dataObjectCapabilities": {},
            }
        ],
        "supportedCompression": "string",
        "supportedFormats": ["xml"],
        "sessionId": b"360559be-0634-47",
        "currentDateTime": int(datetime.utcnow().timestamp()),
        "endpointCapabilities": {},
        "earliestRetainedChangeTime": int(datetime.utcnow().timestamp()),
    }

    schemaHeader = json.loads(mh.avro_schema)
    schemaReqSess = json.loads(rs.avro_schema)
    schemaOpenSess = json.loads(op.avro_schema)

    try:
        # Writing
        with open("test_unserialAvro_header.avro", "wb") as out:
            schemaless_writer(out, schemaHeader, recordsHeader[0])
        print("====== header ======")
        # Reading
        with open("test_unserialAvro_header.avro", "rb") as fo:
            for record in schemaless_reader(fo, schemaHeader):
                print(record)

        # Writing
        with open("test_unserialAvro_reqSess.avro", "wb") as out:
            schemaless_writer(out, schemaReqSess, recordsReqSess[0])
        print("====== reqSess ======")
        # Reading
        with open("test_unserialAvro_reqSess.avro", "rb") as fo:
            for record in schemaless_reader(fo, schemaReqSess):
                print(record)

        # Writing
        with open("test_unserialAvro_openSess.avro", "wb") as out:
            schemaless_writer(out, schemaOpenSess, recordsOpenSess)
        print("====== openSess ======")
        # print(inspect.signature(OpenSession.__init__))
        # print(inspect.signature(RequestSession.__init__))
        # Reading
        with open("test_unserialAvro_openSess.avro", "rb") as fo:
            for record in schemaless_reader(fo, schemaOpenSess):
                print(record)
    finally:
        os.remove("test_unserialAvro_header.avro")
        os.remove("test_unserialAvro_reqSess.avro")
        os.remove("test_unserialAvro_openSess.avro")

    print("====== FIN ======")

    # print(RequestSession.parse_obj(recordsReqSess))
    print(">> " + str(type(op.OpenSession.parse_obj(recordsOpenSess))))


def test_unserialAvro():
    recordsHeader = [
        {
            "protocol": 3,
            "messageType": 1,
            "correlationId": 0,
            "messageId": 2,
            "messageFlags": 0,
        }
    ]
    recordsGetResources = [
        {
            "context": {
                "uri": "eml:///",
                "depth": 10,
                "dataObjectTypes": [],
                "navigableEdges": "Primary",
            },
            "scope": "self",
            "countObjects": True,
            "storeLastWriteFilter": None,
            "activeStatusFilter": "Inactive",
            "includeEdges": False,
        }
    ]

    schemaHeader = json.loads(mh.avro_schema)
    schemaGetResources = json.loads(gr.avro_schema)

    try:
        # Writing
        with open("test_unserialAvro_header.avro", "wb") as out:
            writer(out, schemaHeader, recordsHeader)
        # Reading
        with open("test_unserialAvro_header.avro", "rb") as fo:
            for record in reader(fo):
                print(record)

        # Writing
        with open("test_unserialAvro_reqSess.avro", "wb") as out:
            writer(out, schemaGetResources, recordsGetResources)
        # Reading
        with open("test_unserialAvro_reqSess.avro", "rb") as fo:
            for record in reader(fo):
                print(record)
    finally:
        os.remove("test_unserialAvro_header.avro")
        os.remove("test_unserialAvro_reqSess.avro")


# test_unserialAvroRequestSession()
# test_unserialAvro()
