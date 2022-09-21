import json
from datetime import datetime
from io import BytesIO

import etptypes.energistics.etp.v12.datatypes.message_header as mh
import etptypes.energistics.etp.v12.protocol.core.open_session as os
import etptypes.energistics.etp.v12.protocol.discovery.get_resources as gt
import etptypes.energistics.etp.v12.protocol.data_array.put_data_arrays as pda
import pytest
from fastavro import reader, schemaless_reader, schemaless_writer, writer
from etptypes.energistics.etp.v12.datatypes.any_array import AnyArray
from etptypes.energistics.etp.v12.datatypes.array_of_float import ArrayOfFloat
from etptypes.energistics.etp.v12.datatypes.array_of_double import (
    ArrayOfDouble,
)
from etptypes.energistics.etp.v12.datatypes.array_of_long import ArrayOfLong
from etptypes.energistics.etp.v12.datatypes.array_of_int import ArrayOfInt
from etptypes.energistics.etp.v12.datatypes.array_of_string import (
    ArrayOfString,
)
from etptypes.energistics.etp.v12.datatypes.array_of_boolean import (
    ArrayOfBoolean,
)


def testAvroSerial():
    records = [
        {
            "protocol": 3,
            "messageType": 1,
            "correlationId": 0,
            "messageId": 2,
            "messageFlags": 0,
        }
    ]

    schema = json.loads(mh.avro_schema)

    print(records)
    print("")

    fo = BytesIO(
        b'{"protocol":"3","messageType":"1","correlationId":"0","messageId":"2","messageFlags":"0"}'
    )
    fo = BytesIO()
    writer(fo, schema, records)

    fo.seek(0)
    print(">>")
    print(fo.getvalue())
    reader = schemaless_reader(fo, schema)
    for record in reader:
        print(record)
        print(reader[record])


def testAvroSerialOpenSession():
    records = [
        {
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
    ]

    schema = json.loads(os.avro_schema)

    print(records)
    print("")

    fo = BytesIO()
    writer(fo, schema, records)

    fo.seek(0)
    print(">>")
    print(fo.getvalue())
    read_values = reader(fo, schema)
    for record in read_values:
        print(record)
        # print(read_values[record])


def testPutDataArray_float():
    records = [
        {
            "dataArrays": {
                "0": {
                    "uid": {
                        "uri": "eml:///dataspace('usecase1-1')/resqml22.TriangulatedSetRepresentation(22222222-966d-427f-b780-bdaf26b494dd)",
                        "pathInResource": "resqml/22222222-966d-427f-b780-bdaf26b494dd/test",
                    },
                    "array": {
                        "dimensions": [5],
                        "data": {
                            "item": {"values": [1.0, 2.0, 3.3, 4.4, 5.5]}
                        },
                    },
                    "customData": {},
                }
            }
        }
    ]
    schema = json.loads(pda.avro_schema)

    fo = BytesIO()
    schemaless_writer(fo, schema, records[0])

    fo.seek(0)
    pda_value = pda.PutDataArrays.parse_obj(schemaless_reader(fo, schema))
    assert isinstance(
        pda_value.data_arrays["0"].array.data.item, ArrayOfFloat
    ) or isinstance(pda_value.data_arrays["0"].array.data.item, ArrayOfDouble)


def testPutDataArray_int():
    records = [
        {
            "dataArrays": {
                "0": {
                    "uid": {
                        "uri": "eml:///dataspace('usecase1-1')/resqml22.TriangulatedSetRepresentation(22222222-966d-427f-b780-bdaf26b494dd)",
                        "pathInResource": "resqml/22222222-966d-427f-b780-bdaf26b494dd/test",
                    },
                    "array": {
                        "dimensions": [5],
                        "data": {"item": {"values": [1, 2, 3, 4, 5]}},
                    },
                    "customData": {},
                }
            }
        }
    ]
    schema = json.loads(pda.avro_schema)

    fo = BytesIO()
    schemaless_writer(fo, schema, records[0])

    fo.seek(0)
    pda_value = pda.PutDataArrays.parse_obj(schemaless_reader(fo, schema))
    assert isinstance(
        pda_value.data_arrays["0"].array.data.item, ArrayOfInt
    ) or isinstance(pda_value.data_arrays["0"].array.data.item, ArrayOfLong)


def testPutDataArray_bool():
    records = [
        {
            "dataArrays": {
                "0": {
                    "uid": {
                        "uri": "eml:///dataspace('usecase1-1')/resqml22.TriangulatedSetRepresentation(22222222-966d-427f-b780-bdaf26b494dd)",
                        "pathInResource": "resqml/22222222-966d-427f-b780-bdaf26b494dd/test",
                    },
                    "array": {
                        "dimensions": [5],
                        "data": {
                            "item": {
                                "values": [True, True, False, True, False]
                            }
                        },
                    },
                    "customData": {},
                }
            }
        }
    ]
    schema = json.loads(pda.avro_schema)

    fo = BytesIO()
    schemaless_writer(fo, schema, records[0])

    fo.seek(0)
    pda_value = pda.PutDataArrays.parse_obj(schemaless_reader(fo, schema))
    assert isinstance(
        pda_value.data_arrays["0"].array.data.item, ArrayOfBoolean
    )


def testPutDataArray_str():
    records = [
        {
            "dataArrays": {
                "0": {
                    "uid": {
                        "uri": "eml:///dataspace('usecase1-1')/resqml22.TriangulatedSetRepresentation(22222222-966d-427f-b780-bdaf26b494dd)",
                        "pathInResource": "resqml/22222222-966d-427f-b780-bdaf26b494dd/test",
                    },
                    "array": {
                        "dimensions": [5],
                        "data": {
                            "item": {
                                "values": ["1.0", "2.0", "3.3", "4.4", "5.5"]
                            }
                        },
                    },
                    "customData": {},
                }
            }
        }
    ]
    schema = json.loads(pda.avro_schema)

    fo = BytesIO()
    schemaless_writer(fo, schema, records[0])

    fo.seek(0)
    pda_value = pda.PutDataArrays.parse_obj(schemaless_reader(fo, schema))
    assert isinstance(
        pda_value.data_arrays["0"].array.data.item, ArrayOfString
    )


def notatestAvroSerialGetResources():
    records = [
        {
            "context": {
                "uri": "eml:///",
                "depth": 1,
                "dataObjectTypes": [],
                "navigableEdges": "Primary",
                "includeSecondaryTargets": False,
                "includeSecondarySources": False,
            },
            "scope": "self",
            "countObjects": True,
            "storeLastWriteFilter": None,
            "activeStatusFilter": None,
            "includeEdges": False,
        }
    ]

    schema = json.loads(gt.avro_schema)

    print(records)
    print("")

    fo = BytesIO()
    schemaless_writer(fo, schema, records[0])

    fo.seek(0)
    print(">> ", fo.getvalue())
    reader = schemaless_reader(fo, schema)
    print("TYPE : ", gt.GetResources.parse_obj(reader))
    for record in reader:
        print(record)
        print(reader[record])


def notatestPutDataArray():
    records = [
        {
            "dataArrays": {
                "0": {
                    "uid": {
                        "uri": "eml:///dataspace('usecase1-1')/resqml22.TriangulatedSetRepresentation(22222222-966d-427f-b780-bdaf26b494dd)",
                        "pathInResource": "resqml/22222222-966d-427f-b780-bdaf26b494dd/test",
                    },
                    "array": {
                        "dimensions": [5],
                        "data": {
                            "item": {
                                # "values": [True, True, Fals   e, True, False]
                                # "values": ["1.0", "2.0", "3.3", "4.4", "5.5"]
                                # "values": [1, 2, 3, 4, 5]
                                "values": [1.0, 2.0, 3.3, 4.4, 5.5]
                            }
                        },
                    },
                    "customData": {},
                }
            }
        }
    ]
    schema = json.loads(pda.avro_schema)

    print(records)
    print("")

    fo = BytesIO()
    schemaless_writer(fo, schema, records[0])

    fo.seek(0)
    print(">> ", fo.getvalue())
    reader = schemaless_reader(fo, schema)  # , return_record_name=True)

    #######################
    from fastavro.validation import validate
    from etptypes import avro_schema

    validate(pda.PutDataArrays.parse_obj(reader), pda.avro_schema)
    #######################

    return

    print("-----\n")
    print(reader)
    print("-- TYPE --\n")
    print(reader["dataArrays"]["0"]["array"]["data"]["item"])
    print(type(reader["dataArrays"]["0"]["array"]["data"]["item"]))
    print(type(reader["dataArrays"]["0"]["array"]["data"]["item"]["values"]))
    print("--AVEC PARSED_OBJ---\n")
    print("TYPE : ", pda.PutDataArrays.parse_obj(reader))

    print("==", AnyArray(item=ArrayOfFloat(values=[1.0, 2.0, 3.3, 4.4, 5.5])))

    # print("TYPE ", type([1.0, 2.0, 3.3, 4.4, 5.5]), type(([1.0, 2.0, 3.3, 4.4, 5.5])[0]))

    import numpy as np

    np_array_test = np.random.random((10)).astype(np.float32)
    np_array_test_dbl = np.random.random((10)).astype(np.float64)
    np_array_test_long = np.random.random((10)).astype(np.int64)
    np_array_test_int = np.random.random((10)).astype(np.int32)
    # np_array_test = np.empty([5], dtype = np.float32)

    print(
        "=1d=",
        AnyArray(
            item=ArrayOfDouble(values=np_array_test_dbl.flatten().tolist())
        ),
    )
    # print("=1d=", AnyArray(item=ArrayOfDouble(values=list(np_array_test_dbl.flatten()) )))
    # print("=1d=", AnyArray(item=ArrayOfDouble(values=getattr(np_array_test_dbl.flatten(), "tolist", lambda: value)() )))

    print(
        "=1f=",
        AnyArray(item=ArrayOfFloat(values=np_array_test.flatten().tolist())),
    )
    # print("=1f=", AnyArray(item=ArrayOfFloat(values=list(np_array_test.flatten()) )))
    # print("=1f=", AnyArray(item=ArrayOfFloat(values=np_array_test.flatten().tolist() )))
    # print("=1f=", AnyArray(item=ArrayOfFloat(values=getattr(np_array_test.flatten(), "tolist", lambda: value)() )))
    # print("=2=", AnyArray(item=ArrayOfFloat(values=list(np_array_test.flatten()) )))

    # print("=1L=", AnyArray(item=ArrayOfLong(values=list(np_array_test_long.flatten()) )))
    print(
        "=1L=",
        AnyArray(
            item=ArrayOfLong(values=np_array_test_long.flatten().tolist())
        ),
    )

    print(
        "=1d=",
        AnyArray(item=ArrayOfInt(values=np_array_test_int.flatten().tolist())),
    )

    # for record in reader:
    #     print(record)
    #     print(reader[record])


def _testPutDataArray_record_name():
    records = [
        {
            "dataArrays": {
                "0": {
                    "uid": {
                        "uri": "eml:///dataspace('usecase1-1')/resqml22.TriangulatedSetRepresentation(22222222-966d-427f-b780-bdaf26b494dd)",
                        "pathInResource": "resqml/22222222-966d-427f-b780-bdaf26b494dd/test",
                    },
                    "array": {
                        "dimensions": [5],
                        "data": {
                            "item": {
                                # "values": [True, True, Fals   e, True, False]
                                # "values": ["1.0", "2.0", "3.3", "4.4", "5.5"]
                                # "values": [1, 2, 3, 4, 5]
                                "values": [1.0, 2.0, 3.3, 4.4, 5.5]
                            }
                        },
                    },
                    "customData": {},
                }
            }
        }
    ]
    schema = json.loads(pda.avro_schema)

    # print(records)
    # print("")

    fo = BytesIO()
    schemaless_writer(fo, schema, records[0])

    fo.seek(0)
    print(">> ", fo.getvalue())
    reader = schemaless_reader(fo, schema, return_record_name=True)

    print(reader)
    print("--AVEC PARSED_OBJ---\n")
    fo.seek(0)
    rr = schemaless_reader(fo, schema)
    print("--> ", rr)
    pda_value = pda.PutDataArrays.parse_obj(rr)
    print(
        "==> ",
        pda_value,
        "\n",
        type(list(pda_value.data_arrays.keys())[0]),
        " __ ",
        list(pda_value.data_arrays.keys())[0] == "0",
    )
    print("==> ", pda_value, "\n", type(pda_value.data_arrays))
    print("==> ", pda_value.data_arrays["0"].array.data.item)

    print("-- OTHER ---\n")
    print("==", AnyArray(item=ArrayOfFloat(values=[1.0, 2.0, 3.3, 4.4, 5.5])))


if __name__ == "__main__":

    # print("Testing OpenSession")
    # testAvroSerialOpenSession()

    # print("Testing GetResources")
    # testAvroSerialGetResources()

    # print("Testing PutDataArray")
    # notatestPutDataArray()
    # notatestPutDataArray_record_name()
    _testPutDataArray_record_name()
