from etptypes.energistics.etp.v12.protocol.data_array.get_data_arrays import GetDataArrays

from etpproto.messages import Message

get_data_array_0 = GetDataArrays.parse_obj({
    "dataArrays": {
        "0": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch0"
        },
        "1": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch1"
        },
        "2": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch2"
        },
        "3": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch3"
        },
        "4": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch4"
        },
        "5": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch5"
        },
        "6": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch6"
        },
        "7": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch7"
        },
        "8": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch8"
        },
        "9": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch9"
        },
        "10": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch10"
        },
        "11": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch11"
        },
        "12": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch12"
        },
        "13": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch13"
        },
        "14": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch14"
        },
        "15": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch15"
        },
        "16": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch16"
        },
        "17": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch17"
        },
        "18": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch18"
        },
        "19": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch19"
        },
        "20": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch20"
        },
        "21": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch21"
        },
        "22": {
            "uri": "eml:///dataspace('volve-eqn-plus')/resqml22.TriangulatedSetRepresentation(c6ec3a44-37a3-421f-ade6-91b2ced532e8)",
            "pathInResource": "RESQML/c6ec3a44-37a3-421f-ade6-91b2ced532e8/point_patch22"
        }
    }
})

if __name__ == "__main__":
    print(get_data_array_0)
    Message.get_object_message()
