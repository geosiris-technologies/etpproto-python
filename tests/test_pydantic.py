import json
from etptypes.energistics.etp.v12.datatypes.array_of_float import ArrayOfFloat


def test_array_schema():
    scheme = ArrayOfFloat.schema_json()
    assert scheme == json.dumps(
        {
            "title": "ArrayOfFloat",
            "type": "object",
            "properties": {
                "values": {
                    "title": "Values",
                    "type": "array",
                    "items": {"type": "number"},
                }
            },
            "required": ["values"],
        }
    )
