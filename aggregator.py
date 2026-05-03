#!/usr/bin/env python3
"""Foxglove WebSocket protocol v1 aggregator.

This exposes one Foxglove WebSocket server to Studio and relays topic data from
an optional ROS1 bridge plus zero or more custom Foxglove SDK servers.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import signal
import struct
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import websockets
from websockets.exceptions import ConnectionClosed
from websockets.server import WebSocketServerProtocol

try:
    import yaml
except ImportError:  # pragma: no cover - yaml is optional for CLI-only use.
    yaml = None


FOXGLOVE_V1_SUBPROTOCOL = "foxglove.websocket.v1"
SDK_V1_SUBPROTOCOL = "foxglove.sdk.v1"

SERVER_BINARY_MESSAGE_DATA = 1
SERVER_BINARY_FETCH_ASSET_RESPONSE = 4
CLIENT_BINARY_MESSAGE_DATA = 1


@dataclass
class CustomConfig:
    name: str
    url: str
    topic_prefixes: list[str] = field(default_factory=list)
    package_prefixes: list[str] = field(default_factory=list)
    default: bool = False


@dataclass
class AppConfig:
    listen: str = "0.0.0.0:8765"
    ros_url: str | None = None
    custom: list[CustomConfig] = field(default_factory=list)
    default_custom: str | None = None
    local_asset_root: Path | None = None


@dataclass
class ChannelRef:
    upstream: str
    upstream_channel_id: int
    frontend_channel_id: int
    topic: str
    channel_msg: dict[str, Any]


@dataclass
class SubscriptionRef:
    client_id: int
    upstream: str
    frontend_subscription_id: int
    upstream_subscription_id: int
    frontend_channel_id: int


@dataclass
class SharedSubscriptionRef:
    upstream: str
    frontend_channel_id: int
    upstream_subscription_id: int
    frontend_subscribers: set[tuple[int, int]] = field(default_factory=set)


@dataclass
class ClientPublishRef:
    client_id: int
    upstream: str
    frontend_channel_id: int
    upstream_channel_id: int


@dataclass
class AssetRequestRef:
    client_id: int
    frontend_request_id: int


class IdAllocator:
    def __init__(self, start: int = 1) -> None:
        self._next = start

    def next(self) -> int:
        value = self._next
        self._next += 1
        return value


class FrontendClient:
    def __init__(self, client_id: int, ws: WebSocketServerProtocol) -> None:
        self.id = client_id
        self.ws = ws

    async def send_json(self, payload: dict[str, Any]) -> None:
        await self.ws.send(json.dumps(payload, separators=(",", ":")))

    async def send_binary(self, payload: bytes) -> None:
        await self.ws.send(payload)


class Upstream:
    def __init__(
        self,
        aggregator: "Aggregator",
        name: str,
        url: str,
        kind: str,
        custom_config: CustomConfig | None = None,
    ) -> None:
        self.aggregator = aggregator
        self.name = name
        self.url = url
        self.kind = kind
        self.custom_config = custom_config
        self.ws: websockets.WebSocketClientProtocol | None = None
        self.connected = False
        self.channels_by_upstream_id: dict[int, ChannelRef] = {}
        self.client_publish_by_frontend_id: dict[tuple[int, int], ClientPublishRef] = {}
        self.client_publish_by_upstream_id: dict[int, ClientPublishRef] = {}
        self.subscription_ids_by_frontend: dict[tuple[int, int], int] = {}
        self.asset_requests_by_upstream_id: dict[int, AssetRequestRef] = {}
        self.subscription_ids = IdAllocator(1)
        self.client_channel_ids = IdAllocator(1)
        self.asset_request_ids = IdAllocator(1)
        self.capabilities: list[str] = []
        self.supported_encodings: list[str] = []

    async def run_forever(self) -> None:
        while True:
            try:
                await self._connect_and_read()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("upstream %s connection failed", self.name)
            await self.aggregator.upstream_disconnected(self)
            await asyncio.sleep(1.0)

    async def _connect_and_read(self) -> None:
        logging.info("connecting upstream %s at %s", self.name, self.url)
        # subprotocols = (
        #     [SDK_V1_SUBPROTOCOL]
        #     if self.kind == "custom"
        #     else [FOXGLOVE_V1_SUBPROTOCOL]
        # )
        async with websockets.connect(
            self.url,
            subprotocols=[FOXGLOVE_V1_SUBPROTOCOL, SDK_V1_SUBPROTOCOL],
            max_size=None,
            ping_interval=20,
            ping_timeout=20,
        ) as ws:
            self.ws = ws
            self.connected = True
            logging.info("upstream %s connected with subprotocol %s", self.name, ws.subprotocol)
            async for message in ws:
                await self.aggregator.handle_upstream_message(self, message)

    async def send_json(self, payload: dict[str, Any]) -> bool:
        if self.ws is None or not self.connected:
            return False
        await self.ws.send(json.dumps(payload, separators=(",", ":")))
        return True

    async def send_binary(self, payload: bytes) -> bool:
        if self.ws is None or not self.connected:
            return False
        await self.ws.send(payload)
        return True


class Aggregator:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.upstreams: dict[str, Upstream] = {}
        self.frontend_clients: dict[int, FrontendClient] = {}
        self.channels_by_frontend_id: dict[int, ChannelRef] = {}
        self.shared_subscriptions_by_upstream_id: dict[tuple[str, int], SharedSubscriptionRef] = {}
        self.shared_subscriptions_by_channel: dict[tuple[str, int], SharedSubscriptionRef] = {}
        self.subscriptions_by_frontend_id: dict[tuple[int, int], SubscriptionRef] = {}
        self.frontend_channel_ids = IdAllocator(1)
        self.frontend_client_ids = IdAllocator(1)
        self._lock = asyncio.Lock()

        if config.ros_url:
            self.upstreams["ros"] = Upstream(self, "ros", config.ros_url, "ros")
        for custom in config.custom:
            self.upstreams[custom.name] = Upstream(self, custom.name, custom.url, "custom", custom)

    def server_capabilities(self) -> list[str]:
        capabilities: list[str] = []
        if self.upstreams.get("ros"):
            # Report clientPublish capability if ROS1 bridge is enabled. Custom servers only support assets but not clientPublish.
            capabilities.append("clientPublish")
        if self.config.custom or self.config.local_asset_root:
            # Report asset capability if any custom server is enabled or local asset root is configured, so that Studio can fetch assets from the aggregator. Not fetch assets from ros1 here.
            capabilities.append("assets")
        return capabilities

    def supported_encodings(self) -> list[str]:
        if not self.upstreams:
            return []
        encodings = {"json", "protobuf", "flatbuffer", "ros1"}
        return sorted(encodings)

    def server_metadata(self) -> dict[str, str]:
        if self.upstreams.get("ros"):
            # Important to report this supports ROS1 for foxglove studio client to configure ROS1-compatible features like click tools in the 3D panel.
            return {"ROS_DISTRO": "noetic"}
        else:
            return {}

    async def serve(self) -> None:
        host, port = parse_listen(self.config.listen)
        upstream_tasks = [asyncio.create_task(upstream.run_forever()) for upstream in self.upstreams.values()]
        stop = asyncio.Future()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set_result, None)

        async with websockets.serve(
            self.handle_frontend,
            host,
            port,
            subprotocols=[FOXGLOVE_V1_SUBPROTOCOL, SDK_V1_SUBPROTOCOL],
            max_size=None,
        ):
            logging.info("aggregator listening on ws://%s:%d", host, port)
            await stop

        for task in upstream_tasks:
            task.cancel()
        await asyncio.gather(*upstream_tasks, return_exceptions=True)

    async def handle_frontend(self, ws: WebSocketServerProtocol) -> None:
        client = FrontendClient(self.frontend_client_ids.next(), ws)
        self.frontend_clients[client.id] = client
        logging.info("frontend client %s connected", client.id)
        try:
            # send once when a client (foxglove studio) connects.
            await client.send_json(
                {
                    "op": "serverInfo",
                    "name": "foxglove-flexible-aggregator",
                    "capabilities": self.server_capabilities(),
                    "supportedEncodings": self.supported_encodings(),
                    "metadata": self.server_metadata(),
                    "sessionId": str(int(time.time() * 1000)),
                }
            )
            await self.send_existing_channels(client)
            async for message in ws:
                # keep handling messages until the client disconnects.
                await self.handle_frontend_message(client, message)
        except ConnectionClosed:
            pass
        finally:
            await self.frontend_disconnected(client)
            logging.info("frontend client %s disconnected", client.id)

    async def send_existing_channels(self, client: FrontendClient) -> None:
        channels_by_upstream: dict[str, list[dict[str, Any]]] = {}
        async with self._lock:
            for ref in self.channels_by_frontend_id.values():
                upstream = self.upstreams.get(ref.upstream)
                if upstream is None:
                    continue
                channels_by_upstream.setdefault(ref.upstream, []).append(dict(ref.channel_msg))
        for channels in channels_by_upstream.values():
            if channels:
                await client.send_json({"op": "advertise", "channels": channels})

    async def frontend_disconnected(self, client: FrontendClient) -> None:
        self.frontend_clients.pop(client.id, None)

        to_unsubscribe: dict[str, list[int]] = {}
        to_unadvertise: dict[str, list[int]] = {}
        async with self._lock:
            for key, ref in list(self.subscriptions_by_frontend_id.items()):
                if ref.client_id == client.id:
                    shared_key = (ref.upstream, ref.frontend_channel_id)
                    shared_ref = self.shared_subscriptions_by_channel.get(shared_key)
                    if shared_ref is not None:
                        shared_ref.frontend_subscribers.discard((ref.client_id, ref.frontend_subscription_id))
                        if not shared_ref.frontend_subscribers:
                            self.shared_subscriptions_by_channel.pop(shared_key, None)
                            self.shared_subscriptions_by_upstream_id.pop(
                                (ref.upstream, ref.upstream_subscription_id), None
                            )
                            to_unsubscribe.setdefault(ref.upstream, []).append(ref.upstream_subscription_id)
                    self.subscriptions_by_frontend_id.pop(key, None)
            for upstream in self.upstreams.values():
                for ref in list(upstream.client_publish_by_frontend_id.values()):
                    if ref.client_id == client.id:
                        to_unadvertise.setdefault(upstream.name, []).append(ref.upstream_channel_id)
                        upstream.client_publish_by_frontend_id.pop(
                            (ref.client_id, ref.frontend_channel_id), None
                        )
                        upstream.client_publish_by_upstream_id.pop(ref.upstream_channel_id, None)

        for upstream_name, subscription_ids in to_unsubscribe.items():
            upstream = self.upstreams.get(upstream_name)
            if upstream and subscription_ids:
                await upstream.send_json({"op": "unsubscribe", "subscriptionIds": subscription_ids})
        for upstream_name, channel_ids in to_unadvertise.items():
            upstream = self.upstreams.get(upstream_name)
            if upstream and channel_ids:
                await upstream.send_json({"op": "unadvertise", "channelIds": channel_ids})

    async def handle_frontend_message(self, client: FrontendClient, message: str | bytes) -> None:
        if isinstance(message, bytes):
            await self.handle_frontend_binary(client, message)
            return

        try:
            payload = json.loads(message)
        except json.JSONDecodeError as exc:
            await client.send_json(status("error", f"Invalid JSON from client: {exc}"))
            return

        op = payload.get("op")
        if op == "subscribe":
            await self.handle_subscribe(client, payload)
        elif op == "unsubscribe":
            await self.handle_unsubscribe(client, payload)
        elif op == "advertise":
            await self.handle_client_advertise(client, payload)
        elif op == "unadvertise":
            await self.handle_client_unadvertise(client, payload)
        elif op == "fetchAsset":
            await self.handle_fetch_asset(client, payload)
        elif op in {
            "getParameters",
            "setParameters",
            "subscribeParameterUpdates",
            "unsubscribeParameterUpdates",
            "subscribeConnectionGraph",
            "unsubscribeConnectionGraph",
        }:
            await client.send_json(status("error", f"Aggregator does not support {op}"))
        else:
            await client.send_json(status("warning", f"Aggregator ignored unsupported op {op!r}"))

    async def handle_subscribe(self, client: FrontendClient, payload: dict[str, Any]) -> None:
        by_upstream: dict[str, list[dict[str, int]]] = {}
        async with self._lock:
            for subscription in payload.get("subscriptions", []):
                frontend_subscription_id = int(subscription["id"])
                frontend_channel_id = int(subscription["channelId"])
                channel_ref = self.channels_by_frontend_id.get(frontend_channel_id)
                if channel_ref is None:
                    await client.send_json(status("error", f"Unknown channel id {frontend_channel_id}"))
                    continue
                upstream = self.upstreams[channel_ref.upstream]
                shared_key = (upstream.name, frontend_channel_id)
                shared_ref = self.shared_subscriptions_by_channel.get(shared_key)
                if shared_ref is None:
                    upstream_subscription_id = upstream.subscription_ids.next()
                    shared_ref = SharedSubscriptionRef(
                        upstream=upstream.name,
                        frontend_channel_id=frontend_channel_id,
                        upstream_subscription_id=upstream_subscription_id,
                    )
                    self.shared_subscriptions_by_channel[shared_key] = shared_ref
                    self.shared_subscriptions_by_upstream_id[
                        (upstream.name, upstream_subscription_id)
                    ] = shared_ref
                    by_upstream.setdefault(upstream.name, []).append(
                        {"id": upstream_subscription_id, "channelId": channel_ref.upstream_channel_id}
                    )
                else:
                    upstream_subscription_id = shared_ref.upstream_subscription_id
                ref = SubscriptionRef(
                    client_id=client.id,
                    upstream=upstream.name,
                    frontend_subscription_id=frontend_subscription_id,
                    upstream_subscription_id=upstream_subscription_id,
                    frontend_channel_id=frontend_channel_id,
                )
                self.subscriptions_by_frontend_id[(client.id, frontend_subscription_id)] = ref
                shared_ref.frontend_subscribers.add((client.id, frontend_subscription_id))

        for upstream_name, subscriptions in by_upstream.items():
            upstream = self.upstreams[upstream_name]
            if not await upstream.send_json({"op": "subscribe", "subscriptions": subscriptions}):
                await client.send_json(status("error", f"Upstream {upstream_name} is unavailable"))

    async def handle_unsubscribe(self, client: FrontendClient, payload: dict[str, Any]) -> None:
        by_upstream: dict[str, list[int]] = {}
        async with self._lock:
            for frontend_subscription_id in payload.get("subscriptionIds", []):
                key = (client.id, int(frontend_subscription_id))
                ref = self.subscriptions_by_frontend_id.pop(key, None)
                if ref is None:
                    continue
                shared_key = (ref.upstream, ref.frontend_channel_id)
                shared_ref = self.shared_subscriptions_by_channel.get(shared_key)
                if shared_ref is None:
                    continue
                shared_ref.frontend_subscribers.discard((ref.client_id, ref.frontend_subscription_id))
                if not shared_ref.frontend_subscribers:
                    self.shared_subscriptions_by_channel.pop(shared_key, None)
                    self.shared_subscriptions_by_upstream_id.pop((ref.upstream, ref.upstream_subscription_id), None)
                    by_upstream.setdefault(ref.upstream, []).append(ref.upstream_subscription_id)

        for upstream_name, subscription_ids in by_upstream.items():
            upstream = self.upstreams.get(upstream_name)
            if upstream is not None:
                await upstream.send_json({"op": "unsubscribe", "subscriptionIds": subscription_ids})

    async def handle_client_advertise(self, client: FrontendClient, payload: dict[str, Any]) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for channel in payload.get("channels", []):
            target = self.route_client_publish(channel)
            if target is None:
                await client.send_json(
                    status(
                        "error",
                        f"No upstream route for clientPublish topic={channel.get('topic')} encoding={channel.get('encoding')}",
                    )
                )
                continue

            upstream_channel = dict(channel)
            frontend_channel_id = int(upstream_channel["id"])
            upstream_channel_id = target.client_channel_ids.next()
            upstream_channel["id"] = upstream_channel_id
            ref = ClientPublishRef(client.id, target.name, frontend_channel_id, upstream_channel_id)
            target.client_publish_by_frontend_id[(client.id, frontend_channel_id)] = ref
            target.client_publish_by_upstream_id[upstream_channel_id] = ref
            grouped.setdefault(target.name, []).append(upstream_channel)

        for upstream_name, channels in grouped.items():
            upstream = self.upstreams[upstream_name]
            if not await upstream.send_json({"op": "advertise", "channels": channels}):
                await client.send_json(status("error", f"Upstream {upstream_name} is unavailable"))

    async def handle_client_unadvertise(self, client: FrontendClient, payload: dict[str, Any]) -> None:
        grouped: dict[str, list[int]] = {}
        for frontend_channel_id in payload.get("channelIds", []):
            for upstream in self.upstreams.values():
                ref = upstream.client_publish_by_frontend_id.pop((client.id, int(frontend_channel_id)), None)
                if ref is None:
                    continue
                upstream.client_publish_by_upstream_id.pop(ref.upstream_channel_id, None)
                grouped.setdefault(upstream.name, []).append(ref.upstream_channel_id)
                break
        for upstream_name, channel_ids in grouped.items():
            await self.upstreams[upstream_name].send_json({"op": "unadvertise", "channelIds": channel_ids})

    async def handle_frontend_binary(self, client: FrontendClient, data: bytes) -> None:
        if not data:
            return
        opcode = data[0]
        if opcode != CLIENT_BINARY_MESSAGE_DATA:
            await client.send_json(status("error", f"Aggregator does not support client binary opcode {opcode}"))
            return
        if len(data) < 5:
            await client.send_json(status("error", "Malformed client messageData"))
            return

        frontend_channel_id = struct.unpack_from("<I", data, 1)[0]
        for upstream in self.upstreams.values():
            ref = upstream.client_publish_by_frontend_id.get((client.id, frontend_channel_id))
            if ref is None:
                continue
            rewritten = bytearray(data)
            struct.pack_into("<I", rewritten, 1, ref.upstream_channel_id)
            if not await upstream.send_binary(bytes(rewritten)):
                await client.send_json(status("error", f"Upstream {upstream.name} is unavailable"))
            return

        await client.send_json(status("error", f"Unknown clientPublish channel id {frontend_channel_id}"))

    async def handle_fetch_asset(self, client: FrontendClient, payload: dict[str, Any]) -> None:
        request_id = int(payload["requestId"])
        uri = str(payload["uri"])
        upstream = self.route_asset(uri)
        if upstream is not None:
            upstream_request_id = upstream.asset_request_ids.next()
            upstream.asset_requests_by_upstream_id[upstream_request_id] = AssetRequestRef(client.id, request_id)
            ok = await upstream.send_json({"op": "fetchAsset", "uri": uri, "requestId": upstream_request_id})
            if not ok:
                upstream.asset_requests_by_upstream_id.pop(upstream_request_id, None)
                await client.send_binary(encode_fetch_asset_error(request_id, f"Upstream {upstream.name} is unavailable"))
            return

        local_data = self.try_local_asset(uri)
        if local_data is not None:
            await client.send_binary(encode_fetch_asset_success(request_id, local_data))
            return
        await client.send_binary(encode_fetch_asset_error(request_id, f"No asset route for {uri}"))

    async def handle_upstream_message(self, upstream: Upstream, message: str | bytes) -> None:
        if isinstance(message, bytes):
            await self.handle_upstream_binary(upstream, message)
            return

        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            logging.warning("invalid JSON from upstream %s", upstream.name)
            return

        op = payload.get("op")
        if op == "serverInfo":
            print(f"\n\033[032mReceived upstream '{upstream.name}' serverInfo: {payload}\033[0m\n")
            upstream.capabilities = list(payload.get("capabilities", []))
            upstream.supported_encodings = list(payload.get("supportedEncodings", []))
            logging.info("upstream %s serverInfo capabilities=%s", upstream.name, payload.get("capabilities", []))
        elif op == "advertise":
            await self.handle_upstream_advertise(upstream, payload)
        elif op == "unadvertise":
            await self.handle_upstream_unadvertise(upstream, payload)
        elif op == "status":
            await self.broadcast_json(payload)
        elif op in {"parameterValues", "advertiseServices", "unadvertiseServices", "connectionGraphUpdate"}:
            logging.debug("dropping unsupported upstream op %s from %s", op, upstream.name)
        else:
            logging.debug("ignored upstream op %s from %s", op, upstream.name)

    async def handle_upstream_advertise(self, upstream: Upstream, payload: dict[str, Any]) -> None:
        frontend_channels = []
        async with self._lock:
            for channel in payload.get("channels", []):
                upstream_channel_id = int(channel["id"])
                old_ref = upstream.channels_by_upstream_id.get(upstream_channel_id)
                if old_ref is not None:
                    frontend_channel_id = old_ref.frontend_channel_id
                else:
                    frontend_channel_id = self.frontend_channel_ids.next()
                frontend_channel = dict(channel)
                frontend_channel["id"] = frontend_channel_id
                ref = ChannelRef(
                    upstream=upstream.name,
                    upstream_channel_id=upstream_channel_id,
                    frontend_channel_id=frontend_channel_id,
                    topic=str(channel.get("topic", "")),
                    channel_msg=frontend_channel,
                )
                upstream.channels_by_upstream_id[upstream_channel_id] = ref
                self.channels_by_frontend_id[frontend_channel_id] = ref
                frontend_channels.append(frontend_channel)

        if frontend_channels:
            await self.broadcast_json({"op": "advertise", "channels": frontend_channels})

    async def handle_upstream_unadvertise(self, upstream: Upstream, payload: dict[str, Any]) -> None:
        frontend_channel_ids = []
        pending_asset_errors: list[AssetRequestRef] = []
        async with self._lock:
            for upstream_channel_id in payload.get("channelIds", []):
                ref = upstream.channels_by_upstream_id.pop(int(upstream_channel_id), None)
                if ref is None:
                    continue
                self.channels_by_frontend_id.pop(ref.frontend_channel_id, None)
                frontend_channel_ids.append(ref.frontend_channel_id)
        if frontend_channel_ids:
            await self.broadcast_json({"op": "unadvertise", "channelIds": frontend_channel_ids})

    async def handle_upstream_binary(self, upstream: Upstream, data: bytes) -> None:
        if not data:
            return
        opcode = data[0]
        if opcode == SERVER_BINARY_MESSAGE_DATA:
            await self.forward_upstream_message_data(upstream, data)
        elif opcode == SERVER_BINARY_FETCH_ASSET_RESPONSE:
            await self.forward_fetch_asset_response(upstream, data)
        else:
            logging.debug("dropping upstream %s binary opcode %d", upstream.name, opcode)

    async def forward_upstream_message_data(self, upstream: Upstream, data: bytes) -> None:
        if len(data) < 13:
            logging.warning("malformed messageData from %s", upstream.name)
            return
        upstream_subscription_id = struct.unpack_from("<I", data, 1)[0]
        shared_ref = self.shared_subscriptions_by_upstream_id.get((upstream.name, upstream_subscription_id))
        if shared_ref is None:
            return
        for client_id, frontend_subscription_id in list(shared_ref.frontend_subscribers):
            client = self.frontend_clients.get(client_id)
            if client is None:
                continue
            rewritten = bytearray(data)
            struct.pack_into("<I", rewritten, 1, frontend_subscription_id)
            await client.send_binary(bytes(rewritten))

    async def forward_fetch_asset_response(self, upstream: Upstream, data: bytes) -> None:
        if len(data) < 10:
            logging.warning("malformed fetchAssetResponse from %s", upstream.name)
            return
        upstream_request_id = struct.unpack_from("<I", data, 1)[0]
        ref = upstream.asset_requests_by_upstream_id.pop(upstream_request_id, None)
        if ref is None:
            return
        client = self.frontend_clients.get(ref.client_id)
        if client is None:
            return
        rewritten = bytearray(data)
        struct.pack_into("<I", rewritten, 1, ref.frontend_request_id)
        await client.send_binary(bytes(rewritten))

    async def upstream_disconnected(self, upstream: Upstream) -> None:
        if not upstream.connected and not upstream.channels_by_upstream_id:
            return
        logging.warning("upstream %s disconnected", upstream.name)
        upstream.connected = False
        upstream.ws = None
        frontend_channel_ids = []
        async with self._lock:
            for ref in list(upstream.channels_by_upstream_id.values()):
                self.channels_by_frontend_id.pop(ref.frontend_channel_id, None)
                frontend_channel_ids.append(ref.frontend_channel_id)
            upstream.channels_by_upstream_id.clear()

            for key, ref in list(self.shared_subscriptions_by_upstream_id.items()):
                if ref.upstream == upstream.name:
                    self.shared_subscriptions_by_upstream_id.pop(key, None)
                    self.shared_subscriptions_by_channel.pop((ref.upstream, ref.frontend_channel_id), None)
                    for client_id, frontend_subscription_id in ref.frontend_subscribers:
                        self.subscriptions_by_frontend_id.pop((client_id, frontend_subscription_id), None)

            upstream.subscription_ids_by_frontend.clear()
            upstream.client_publish_by_frontend_id.clear()
            upstream.client_publish_by_upstream_id.clear()
            pending_asset_errors = list(upstream.asset_requests_by_upstream_id.values())
            upstream.asset_requests_by_upstream_id.clear()

        if frontend_channel_ids:
            await self.broadcast_json({"op": "unadvertise", "channelIds": frontend_channel_ids})
        for ref in pending_asset_errors:
            client = self.frontend_clients.get(ref.client_id)
            if client is not None:
                with contextlib.suppress(ConnectionClosed):
                    await client.send_binary(
                        encode_fetch_asset_error(
                            ref.frontend_request_id,
                            f"Upstream {upstream.name} disconnected while fetching asset",
                        )
                    )
        await self.broadcast_json(status("warning", f"Upstream {upstream.name} disconnected"))

    async def broadcast_json(self, payload: dict[str, Any]) -> None:
        dead: list[int] = []
        for client in list(self.frontend_clients.values()):
            try:
                await client.send_json(payload)
            except ConnectionClosed:
                dead.append(client.id)
        for client_id in dead:
            self.frontend_clients.pop(client_id, None)

    def route_client_publish(self, channel: dict[str, Any]) -> Upstream | None:
        if channel.get("encoding") == "ros1":
            ros = self.upstreams.get("ros")
            return ros if ros and ros.connected else None

        topic = str(channel.get("topic", ""))
        for upstream in self.upstreams.values():
            if upstream.kind != "custom" or upstream.custom_config is None:
                continue
            if any(topic.startswith(prefix) for prefix in upstream.custom_config.topic_prefixes):
                return upstream if upstream.connected else None

        default = self.default_custom_upstream()
        return default if default and default.connected else None

    def route_asset(self, uri: str) -> Upstream | None:
        package = package_name_from_uri(uri)
        if package is not None:
            for upstream in self.upstreams.values():
                if upstream.kind != "custom" or upstream.custom_config is None:
                    continue
                if package in upstream.custom_config.package_prefixes:
                    return upstream if upstream.connected else None

        default = self.default_custom_upstream()
        return default if default and default.connected else None

    def default_custom_upstream(self) -> Upstream | None:
        if self.config.default_custom:
            upstream = self.upstreams.get(self.config.default_custom)
            if upstream and upstream.kind == "custom":
                return upstream
        for upstream in self.upstreams.values():
            if upstream.kind == "custom" and upstream.custom_config and upstream.custom_config.default:
                return upstream
        for upstream in self.upstreams.values():
            if upstream.kind == "custom":
                return upstream
        return None

    def try_local_asset(self, uri: str) -> bytes | None:
        root = self.config.local_asset_root
        if root is None or not uri.startswith("package://"):
            return None
        rel = uri[len("package://") :]
        path = (root / rel).resolve()
        try:
            path.relative_to(root.resolve())
        except ValueError:
            return None
        if not path.is_file():
            return None
        return path.read_bytes()


def status(level: str, message: str) -> dict[str, Any]:
    levels = {"info": 0, "warning": 1, "error": 2}
    return {"op": "status", "level": levels[level], "message": message}


def encode_fetch_asset_success(request_id: int, data: bytes) -> bytes:
    return bytes([SERVER_BINARY_FETCH_ASSET_RESPONSE]) + struct.pack("<IBI", request_id, 0, 0) + data


def encode_fetch_asset_error(request_id: int, message: str) -> bytes:
    encoded = message.encode("utf-8")
    return bytes([SERVER_BINARY_FETCH_ASSET_RESPONSE]) + struct.pack("<IBI", request_id, 1, len(encoded)) + encoded


def package_name_from_uri(uri: str) -> str | None:
    if not uri.startswith("package://"):
        return None
    rest = uri[len("package://") :]
    return rest.split("/", 1)[0] if rest else None


def parse_listen(value: str) -> tuple[str, int]:
    if "://" in value:
        parsed = urlparse(value)
        return parsed.hostname or "0.0.0.0", parsed.port or 8765
    host, port = value.rsplit(":", 1)
    return host, int(port)


def parse_custom(value: str) -> CustomConfig:
    name, url = value.split("=", 1)
    return CustomConfig(name=name, url=url, topic_prefixes=[f"/{name}/"], package_prefixes=[f"{name}_description"])


def load_config(args: argparse.Namespace) -> AppConfig:
    cfg = AppConfig()
    if args.config:
        if yaml is None:
            raise RuntimeError("PyYAML is required for --config")
        with open(args.config, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        cfg.listen = raw.get("listen", cfg.listen)
        ros = raw.get("ros") or {}
        if ros.get("enabled", bool(ros.get("url"))) and ros.get("url"):
            cfg.ros_url = ros["url"]
        cfg.custom = [
            CustomConfig(
                name=item["name"],
                url=item["url"],
                topic_prefixes=list(item.get("topic_prefixes", [])),
                package_prefixes=list(item.get("package_prefixes", [])),
                default=bool(item.get("default", False)),
            )
            for item in raw.get("custom", [])
        ]
        cfg.default_custom = raw.get("default_custom")
        if raw.get("local_asset_root"):
            cfg.local_asset_root = Path(raw["local_asset_root"]).resolve()

    if args.listen:
        cfg.listen = args.listen
    if args.ros:
        cfg.ros_url = args.ros
    for value in args.custom or []:
        cfg.custom.append(parse_custom(value))
    if args.default_custom:
        cfg.default_custom = args.default_custom
    if args.local_asset_root:
        cfg.local_asset_root = Path(args.local_asset_root).resolve()
    return cfg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="YAML config file")
    parser.add_argument("--listen", help="Listen address, e.g. 0.0.0.0:8765")
    parser.add_argument("--ros", help="ROS1 foxglove_bridge URL")
    parser.add_argument("--custom", action="append", help="Custom upstream as name=ws://host:port")
    parser.add_argument("--default-custom", help="Default custom upstream name")
    parser.add_argument("--local-asset-root", help="Optional local package:// asset root")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(asctime)s %(levelname)s %(message)s")
    config = load_config(args)
    asyncio.run(Aggregator(config).serve())


if __name__ == "__main__":
    main()
