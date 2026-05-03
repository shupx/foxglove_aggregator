#!/usr/bin/env python3
import logging
import math
import time
from pathlib import Path
from typing import Optional

import foxglove
from foxglove.messages import (
    ArrowPrimitive,
    Color,
    CubePrimitive,
    FrameTransform,
    FrameTransforms,
    Pose,
    PoseInFrame,
    Quaternion,
    SceneEntity,
    SceneUpdate,
    Timestamp,
    Vector3,
)

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

    try:
        while True:
            now = time.time()
            stamp = Timestamp.from_epoch_secs(now)
            yaw = now
            position = Vector3(x=2.0 * math.cos(now), y=2.0 * math.sin(now), z=0.8)
            orientation = yaw_to_quaternion(yaw)
            pose = Pose(
                position=position,
                orientation=orientation,
            )

            foxglove.log(
                "/custom_a/pose",
                PoseInFrame(
                    timestamp=stamp,
                    frame_id="map",
                    pose=pose,
                ),
            )

            foxglove.log(
                "/custom_a/markers",
                SceneUpdate(
                    entities=[
                        SceneEntity(
                            timestamp=stamp,
                            frame_id="map",
                            id="moving_cube",
                            frame_locked=True,
                            cubes=[
                                CubePrimitive(
                                    pose=pose,
                                    size=Vector3(x=0.6, y=0.35, z=0.25),
                                    color=Color(r=0.1, g=0.45, b=0.9, a=1.0),
                                )
                            ],
                        ),
                        SceneEntity(
                            timestamp=stamp,
                            frame_id="map",
                            id="heading_arrow",
                            frame_locked=True,
                            arrows=[
                                ArrowPrimitive(
                                    pose=pose,
                                    shaft_length=0.8,
                                    shaft_diameter=0.05,
                                    head_length=0.25,
                                    head_diameter=0.16,
                                    color=Color(r=1.0, g=0.45, b=0.05, a=1.0),
                                )
                            ],
                        ),
                    ]
                ),
            )

            foxglove.log(
                "/custom_a/tf",
                FrameTransforms(
                    transforms=[
                        FrameTransform(
                            timestamp=stamp,
                            parent_frame_id="map",
                            child_frame_id="uav1/base_link",
                            translation=position,
                            rotation=orientation,
                        )
                    ]
                ),
            )
            time.sleep(0.02)
    finally:
        server.stop()


if __name__ == "__main__":
    main()
