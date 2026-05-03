# Foxglove Flexible Aggregator

This is a protocol-level Foxglove WebSocket v1 aggregator. Foxglove Studio connects to one
aggregator URL while the aggregator relays channels from an optional ROS1 `foxglove_bridge` and
zero or more custom Foxglove SDK servers.

## Run

Only ROS1 bridge:

```bash
roslaunch --screen foxglove_bridge foxglove_bridge.launch port:=18766
python3 foxglove_aggregator/aggregator.py --listen 0.0.0.0:18765 --ros ws://127.0.0.1:18766
```

Only custom servers:

```bash
python3 foxglove_aggregator/examples/custom_a_server.py
python3 foxglove_aggregator/aggregator.py \
  --listen 0.0.0.0:18765 \
  --custom custom_a=ws://127.0.0.1:18767 \
  --default-custom custom_a
```

YAML config:

```bash
python3 foxglove_aggregator/aggregator.py --config foxglove_aggregator/config.example.yaml
```

Foxglove Studio connects to:

```text
ws://<host>:18765
```

URDF layer URL:

```text
package://custom_a_description/urdf/custom_robot.urdf
```

## Supported v1 Behavior

- Server channel aggregation: `advertise`, `unadvertise`, `subscribe`, `unsubscribe`, `messageData`.
- Client publish routing: `encoding == "ros1"` routes to ROS bridge; otherwise topic prefix/default custom.
- Assets: `fetchAsset` routes by `package://<package>/...` prefix to custom servers, then default custom, then optional local asset root.
- Optional/no upstreams: the aggregator still starts and reports clear status errors for unsupported routes.

Parameters, services, and connection graph aggregation are intentionally not implemented in v1.
