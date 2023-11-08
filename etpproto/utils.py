# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

import importlib
import inspect
import json
import pkgutil
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Type, Union

import etptypes.energistics.etp.v12.protocol
from etptypes import ETPModel, avro_schema


def list_classes(modulePath: str) -> List[Type[ETPModel]]:
    module = importlib.import_module(modulePath)
    classList = []
    for _, obj in inspect.getmembers(module):
        if inspect.isclass(obj):
            classList.append(obj)
    return classList


MessageDict = DefaultDict[str, Type[ETPModel]]
ProtocolDict = DefaultDict[str, MessageDict]


def get_all_etp_protocol_classes() -> ProtocolDict:
    protocolDict: ProtocolDict = defaultdict(
        lambda: defaultdict(type(ETPModel))
    )
    package = etptypes.energistics.etp.v12.protocol
    for _, modname, _ in pkgutil.walk_packages(
        path=getattr(package, "__path__"),
        prefix=package.__name__ + ".",
        onerror=lambda x: None,
    ):
        for classFound in list_classes(modname):
            try:
                schem = json.loads(avro_schema(classFound))
                protocolId = schem["protocol"]
                messageType = schem["messageType"]
                protocolDict[protocolId][messageType] = classFound
            except Exception:
                pass
    return protocolDict


def get_class_from_protocol_and_name(
    protocol: str, name: str, dict_map_pro_to_class: ProtocolDict
):
    protocol = str(protocol)
    if protocol in dict_map_pro_to_class:
        for msg_t in dict_map_pro_to_class[protocol]:
            if (
                dict_map_pro_to_class[protocol][msg_t].__name__.lower()
                == name.lower()
            ):
                return dict_map_pro_to_class[protocol][msg_t]
    return None


def get_first_dict_attribute_name(obj):
    for att in obj.__dict__:
        if type(getattr(obj, att)) == dict:
            return att
    return None


def get_first_list_attribute_name(obj):
    for att in obj.__dict__:
        if type(getattr(obj, att)) == list:
            return att
    return None


class StringType:
    UPPER = 1
    LOWER = 2
    NUMERIC = 3
    OTHER = 4


def classify(character: str) -> int:
    """String classifier."""
    if character.isupper():
        return StringType.UPPER
    if character.islower():
        return StringType.LOWER
    if character.isnumeric():
        return StringType.NUMERIC

    return StringType.OTHER


def split_words(value: str) -> List[str]:
    """Split a string on new capital letters and not alphanumeric
    characters."""
    words: List[str] = []
    buffer: List[str] = []
    previous = None

    def flush():
        if buffer:
            words.append("".join(buffer))
            buffer.clear()

    for char in value:
        tp = classify(char)
        if tp == StringType.OTHER:
            flush()
        elif not previous or tp == previous:
            buffer.append(char)
        elif tp == StringType.UPPER and previous != StringType.UPPER:
            flush()
            buffer.append(char)
        else:
            buffer.append(char)

        previous = tp

    flush()
    return words


def snake_case(value: str, **kwargs: Any) -> str:
    """Convert the given string to snake case."""
    return "_".join(map(str.lower, split_words(value)))


def pascal_case(value: str, **kwargs: Any) -> str:
    """Convert the given string to pascal case."""
    return "".join(map(str.title, split_words(value)))


def concat(a: Union[List, Dict], b: Union[List, Dict]):
    if isinstance(a, list) and isinstance(b, List):
        return a + b
    elif isinstance(a, dict) and isinstance(b, dict):
        x = a.copy()
        x.update(b)
        return x
    elif isinstance(a, list) and isinstance(b, dict):
        return a + [b[key] for key in b]
