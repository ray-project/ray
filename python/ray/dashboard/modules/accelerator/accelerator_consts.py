accelerators = {
    "GPU": {
        "columns": [
            {
                "label": "GPU",
                "help_info": (
                    "Usage of each GPU device. "
                    "If no GPU usage is detected, here are the potential root causes: "
                    "1. non-GPU Ray image is used on this node. "
                    "Switch to a GPU Ray image and try again. "
                    "2. Non Nvidia GPUs are being used. "
                    "Non Nvidia GPUs' utilizations are not currently supported. "
                    "3. pynvml module raises an exception."
                ),
            },
            {"label": "GRAM"},
        ]
    },
    "NPU": {
        "columns": [
            {
                "label": "NPU",
                "help_info": "Indicates the NPU AI core usage of each device.",
            },
            {"label": "HBM"},
        ]
    },
}