#!/usr/bin/env python3
import json
import logging
import math
import time
from pathlib import Path
from typing import Optional

import foxglove
from foxglove import Channel, Schema
from foxglove.messages import FrameTransform, FrameTransforms, Quaternion, Timestamp, Vector3

ASSET_ROOT = Path(__file__).parent / "assets"
PACKAGE_NAME = "custom_a_description"


def asset_handler(uri: str) -> Optional[bytes]:
    prefix = f"package://{PACKAGE_NAME}/"
    if not uri.startswith(prefix):
        return None

    rel = uri[len(prefix) :]
    path = (ASSET_ROOT / PACKAGE_NAME / rel).resolve()
    try:
        path.relative_to(ASSET_ROOT.resolve())
    except ValueError:
        return None
    if not path.is_file():
        return None
    return path.read_bytes()


def yaw_to_quaternion(yaw: float) -> Quaternion:
    return Quaternion(x=0, y=0, z=math.sin(yaw * 0.5), w=math.cos(yaw * 0.5))


def main() -> None:
    foxglove.set_log_level(logging.INFO)
    server = foxglove.start_server(port=18767, asset_handler=asset_handler)

    example = Channel(
        topic="/custom_a/example",
        message_encoding="json",
        schema=Schema(
            name="custom.Example",
            encoding="jsonschema",
            data=json.dumps(
                {
                    "type": "object",
                    "properties": {
                        "stamp": {"type": "number"},
                        "value": {"type": "number"},
                    },
                }
            ).encode("utf-8"),
        ),
    )

    try:
        while True:
            now = time.time()
            example.log(json.dumps({"stamp": now, "value": math.sin(now)}).encode("utf-8"))
            foxglove.log(
                "/custom_a/tf",
                FrameTransforms(
                    transforms=[
                        FrameTransform(
                            timestamp=Timestamp.from_epoch_secs(now),
                            parent_frame_id="world",
                            child_frame_id="base_link",
                            translation=Vector3(x=0, y=0, z=0),
                            rotation=yaw_to_quaternion(now),
                        )
                    ]
                ),
            )
            time.sleep(0.02)
    finally:
        server.stop()


if __name__ == "__main__":
    main()
