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

Launch ROS1 foxglove bridge:

```bash
roslaunch foxglove_bridge foxglove_bridge.launch port:=18766
```

Launch a custom server:

```bash
python3 examples/custom_a_server.py
```

Launch the aggregator with a YAML config (relay ROS1 foxglove bridge + custom servers):

```bash
python3 aggregator.py --config config.example.yaml
```

## Custom Routing Config

Each custom server entry defines how the aggregator routes requests that do not already belong to
an advertised upstream channel:

```yaml
custom:
  - name: "custom_a"
    url: "ws://127.0.0.1:18767"
    topic_prefixes: ["/custom_a/"]
    package_prefixes: ["custom_a_description"]
    default: true
```

- `name`: unique upstream name, used in logs and `--default-custom`.
- `topic_prefixes`: routes non-`ros1` `clientPublish` topics, e.g. `/custom_a/cmd`.
- `package_prefixes`: routes `fetchAsset` for `package://custom_a_description/...`.
- `default: true`: fallback custom server for unmatched non-`ros1` publish and asset requests.

`encoding == "ros1"` client publish always routes to the ROS1 bridge when configured and connected.

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
## Supported Behavior

- Server channel aggregation: `advertise`, `unadvertise`, `subscribe`, `unsubscribe`, `messageData`.
- Client publish routing: `encoding == "ros1"` routes to ROS bridge; otherwise topic prefix/default custom.
- Assets: `fetchAsset` routes by `package://<package>/...` prefix to custom servers, then default custom, then optional local asset root.
- Optional lifecycle webhook: frontend connection-count changes are sent to `lifecycle_hook_url`.
- Optional/no upstreams: the aggregator still starts and reports clear status errors for unsupported routes.

Parameters, services, and connection graph aggregation are intentionally not implemented in the current version of aggregator.


## Frontend Lifecycle Hook

The aggregator can optionally post frontend connection-count changes to an HTTP webhook. This is
plain HTTP and is not part of the Foxglove WebSocket protocol. If `lifecycle_hook_url` is omitted,
the aggregator does not send lifecycle events.

```yaml
lifecycle_hook_url: "http://127.0.0.1:19287/lifecycle"
```

The webhook receives JSON like:

```json
{
  "event": "frontend_client_count_changed",
  "client_count": 0,
  "timestamp": 1777890000.123
}
```

This is intended for optional coordination, such as clearing external display state after the last
Foxglove Studio client disconnects. The aggregator only reports the event; the webhook server owns
any cleanup policy.
