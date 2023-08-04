# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

import importlib
import inspect
import json
import pkgutil
import sys

import etptypes.energistics.etp.v12.protocol

# from etptypes.Energistics.Etp.v12.Protocol import Core
from etptypes import avro_schema

# from etptypes.Energistics.Etp.v12.Datatypes.Uuid import Uuid


def listClasses(modulePath):
    # print("==> " + modulePath)
    module = importlib.import_module(modulePath)
    classList = []
    for name, obj in inspect.getmembers(module):
        # print("_> " + str(name))
        if inspect.isclass(obj):
            # print(str(name))
            classList.append(obj)
        # if inspect.ismodule(obj):
        # print("--> " + str(obj))
        # classList += listClasses(name)
    return classList


def getAllETPProtocolClasses():
    protocolDict = {}
    package = etptypes.energistics.etp.v12.protocol
    for importer, modname, ispkg in pkgutil.walk_packages(
        path=package.__path__,
        prefix=package.__name__ + ".",
        onerror=lambda x: None,
    ):
        # print("======<>======")
        # print(modname)
        # print("======<>======")
        # listClasses(modname)
        # break

        # print(listClasses(modname))
        for classFound in listClasses(modname):
            try:
                schem = json.loads(avro_schema(classFound))
                protocolId = schem["protocol"]
                messageType = schem["messageType"]
                if protocolId not in protocolDict:
                    protocolDict[protocolId] = {}
                protocolDict[protocolId][messageType] = classFound
                # print(classFound.__name__)
            except Exception:
                pass
    return protocolDict


def testInstanciateGeneric():
    # res = listClasses("etptypes.Energistics.Etp.v12.Protocol")

    # for r in res:
    #     print("> " + str(r))

    protocolDict = getAllETPProtocolClasses()

    assert protocolDict["0"]["1"].__name__ == "RequestSession"
    assert protocolDict["0"]["2"].__name__ == "OpenSession"

    # print("============")
    # print(globals()['RequestSession'])
    # print(globals()['RequestSession']._AVRO_SCHEMA)
    # schemaObj = json.loads(globals()['RequestSession']._AVRO_SCHEMA)
    # print("protocol : " + str(schemaObj["protocol"]))
    # assert schemaObj["protocol"] is '0'


# testInstanciateGeneric()
# b = (-1024).to_bytes(10, byteorder='big', signed=True)
# # c: byte[]
# print(len((b)))
# print(type(b))
