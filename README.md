# Foxglove Flexible Aggregator

This is a protocol-level Foxglove WebSocket v1 aggregator. Foxglove Studio connects to one
aggregator URL while the aggregator relays channels from an optional ROS1 `foxglove_bridge` and
zero or more custom Foxglove SDK servers.

## Architecture

```text
        .--------------------.
        |  Foxglove Studio   |
        |  (inner ws client) |
        '---------+----------'
                  ^
                  | one Foxglove WebSocket connection
                  | subscribe / publish / fetchAsset
                  v
        .--------------------.
        |     Aggregator     |
        | ws://host::8765    |
        |                    |
        |  topic id map      |
        |  subscription map  |
        |  clientPublish map |
        |  asset request map |
        '----+----------+------------------------+----'
             ^          ^          ^ custom topics / assets / 
     ros1    |          |          |  non-ros1 publish
 clientPublish          |          |____________________
             v          v                               v
 .----------------.  .--------------------.  .--------------------.
 | ROS1 foxglove  |  | custom_a SDK server |  | custom_b SDK server |
 | bridge         |  | :18767              |  | :18768              |
 |    :18766      |  |                     |  |                     |
 | ros1 topics    |  | /custom_a/pose      |  | /custom_b/...       |
 | ros1 publish   |  | /custom_a/markers   |  | package://assets    |
 '----------------'  | /custom_a/tf        |  '---------------------'
                     | package://assets    |
                     '---------------------'
```

Routing summary:

```text
server advertise/messageData     upstream -> aggregator -> Studio
Studio subscribe/unsubscribe     Studio -> aggregator -> owning upstream
Studio clientPublish ros1        Studio -> aggregator -> ROS1 bridge
Studio clientPublish non-ros1    Studio -> aggregator -> matching/default custom
Studio fetchAsset package://...  Studio -> aggregator -> matching/default custom
```

## Install

```bash
pip install foxglove-sdk  # python 3.10+
sudo apt install ros-noetic-foxglove-bridge -y
```

## Run

Only ROS1 foxglove bridge:

```bash
roslaunch --screen foxglove_bridge foxglove_bridge.launch port:=18766
python3 aggregator.py --listen 127.0.0.1:8765 --ros ws://127.0.0.1:18766
```

Only custom servers:

```bash
python3 examples/custom_a_server.py
python3 aggregator.py \
  --listen 127.0.0.1:8765 \
  --custom custom_a=ws://127.0.0.1:18767 \
  --default-custom custom_a
```

YAML config (ROS1 foxglove bridge + custom servers):

```bash
python3 aggregator.py --config config.example.yaml
```

Foxglove Studio connects to:

```text
ws://127.0.0.1:8765
```

URDF layer URL:

```text
package://custom_a_description/urdf/custom_robot.urdf
```

The example custom server publishes 3D-friendly topics:

```text
/custom_a/pose     foxglove.PoseInFrame
/custom_a/markers  foxglove.SceneUpdate
/custom_a/tf       foxglove.FrameTransforms
```

## Supported v1 Behavior

- Server channel aggregation: `advertise`, `unadvertise`, `subscribe`, `unsubscribe`, `messageData`.
- Client publish routing: `encoding == "ros1"` routes to ROS bridge; otherwise topic prefix/default custom.
- Assets: `fetchAsset` routes by `package://<package>/...` prefix to custom servers, then default custom, then optional local asset root.
- Optional/no upstreams: the aggregator still starts and reports clear status errors for unsupported routes.

Parameters, services, and connection graph aggregation are intentionally not implemented in the current version of aggregator.
