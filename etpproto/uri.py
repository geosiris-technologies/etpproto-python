# Copyright (c) 2022-2023 Geosiris.
# SPDX-License-Identifier: Apache-2.0

import re
from typing import Match, Optional, Union


class Uri(object):
    def __init__(self, uri: str) -> None:
        self.raw_uri = uri


CANONICAL_DATASPACE_URIS = (
    r"^eml:\/\/\/(?:dataspace\('?(?P<dataspace>[^']*?(?:''[^']*?)*)'?\))?$"
)


class DataspaceUri(Uri):
    """
    The canonical form of the default dataspace URI is:
        eml:///
    The canonical form for all other dataspace URIs is:
        eml:///dataspace('{path}')

    - In addition to named dataspaces, all ETP stores and producers MUST support a default, nameless dataspace, which is identified by the empty string.
    - While the default dataspace MUST be supported, it MAY be empty; that is, it may not have any data objects in it.

    IMPORTANT: The default dataspace is NOT an alias for a named dataspace. It is a simplification for ETP stores and producers that do not need to support named datapsaces.
    """

    def __init__(self, uri: str) -> None:
        super(DataspaceUri, self).__init__(uri)
        p = re.compile(CANONICAL_DATASPACE_URIS)
        result = p.search(uri)
        if result is not None:
            self.dataspace = result.group("dataspace")
        else:
            raise AttributeError


# Val: je separe le pkgName car dans le WS on a un package experimental : proposal
PKG_NAMES_REGXP = "[a-zA-Z]+\\w+"  # "witsml|resqml|prodml|eml"

UUID_REGEX = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
CANONICAL_DATA_OBJECT_URIS = (
    r"^eml:\/\/\/(?:dataspace\('(?P<dataspace>[^']*?(?:''[^']*?)*)'\)\/)?(?P<domain>"
    + PKG_NAMES_REGXP
    + r")(?P<domainVersion>[1-9]\d+)\.(?P<objectType>\w+)\((?:(?P<uuid>"
    + UUID_REGEX
    + r")|uuid=(?P<uuid2>"
    + UUID_REGEX
    + r"),version='(?P<version>[^']*?(?:''[^']*?)*)')\)$"
)


class DataObjectURI(Uri):
    def __init__(self, uri: str) -> None:
        super(DataObjectURI, self).__init__(uri)
        p = re.compile(CANONICAL_DATA_OBJECT_URIS)
        result = p.search(uri)
        if result is not None:
            self.dataspace = result.group("dataspace")
            self.domain = result.group("domain")
            self.domain_version = result.group("domainVersion")
            self.object_type = result.group("objectType")

            if result.group("uuid") is None:
                self.uuid = result.group("uuid2")
            else:
                self.uuid = result.group("uuid")

            self.version = result.group("version")
        else:
            raise AttributeError

    @staticmethod
    def validate(uri: str) -> bool:
        return re.match(CANONICAL_DATA_OBJECT_URIS, uri) is not None


def find_uuid(input: str) -> Optional[str]:
    p = re.compile(UUID_REGEX)
    result = p.search(input)
    if result is not None:
        return result.group() if result else None
    else:
        return None


def parse_uri(uri: str) -> Union[DataObjectURI, DataspaceUri, Uri]:
    try:
        return DataObjectURI(uri)
    except AttributeError:  # si un [..].group ne marche pas
        try:
            return DataspaceUri(uri)
        except AttributeError:
            # logging.error(e2)
            return Uri(uri)
