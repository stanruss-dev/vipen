import asyncio
import websockets
import json
import logging
import os
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


class AgentSession:
    def __init__(self, ws, device_id):
        self.ws = ws
        self.device_id = device_id
        self.signal_q = asyncio.Queue()
        self.ctrl_ws = None


agents = {}


async def agent_recv_loop(session):
    async for raw in session.ws:
        try:
            data = json.loads(raw)
            if data.get("type") == "call_response":
                await session.signal_q.put(data)
            elif session.ctrl_ws is not None:
                try:
                    await session.ctrl_ws.send(raw)
                except Exception:
                    session.ctrl_ws = None
        except Exception as e:
            log.error(f"[agent_recv] {e}")


async def handler(websocket):
    try:
        raw = await asyncio.wait_for(websocket.recv(), timeout=30)
        data = json.loads(raw)
        msg = data.get("type")

        if msg == "register" and data.get("role") == "agent":
            device_id = data.get("device_id", "").strip().upper()
            if not device_id:
                await websocket.send(json.dumps({"type": "error", "msg": "device_id required"}))
                return
            session = AgentSession(websocket, device_id)
            agents[device_id] = session
            await websocket.send(json.dumps({"type": "registered"}))
            log.info(f"+ Agent  {device_id}")
            try:
                await agent_recv_loop(session)
            finally:
                agents.pop(device_id, None)
                log.info(f"- Agent  {device_id}")

        elif msg == "call":
            caller_id = data.get("caller_id", "").upper()
            target_id = data.get("target_id", "").upper()
            session = agents.get(target_id)
            if not session:
                await websocket.send(json.dumps({"type": "error", "msg": f"{target_id} не в сети"}))
                return
            await session.ws.send(json.dumps({"type": "incoming_call", "caller_id": caller_id}))
            try:
                resp = await asyncio.wait_for(session.signal_q.get(), timeout=60)
            except asyncio.TimeoutError:
                await websocket.send(json.dumps({"type": "call_rejected"}))
                return
            if resp.get("accept"):
                session.ctrl_ws = websocket
                await websocket.send(json.dumps({"type": "call_accepted"}))
                log.info(f"Call  {caller_id} -> {target_id}")
                try:
                    async for raw in websocket:
                        try:
                            await session.ws.send(raw)
                        except Exception:
                            break
                except websockets.exceptions.ConnectionClosed:
                    pass
                finally:
                    session.ctrl_ws = None
                    try:
                        await session.ws.send(json.dumps({"type": "agent_disconnected"}))
                    except Exception:
                        pass
            else:
                await websocket.send(json.dumps({"type": "call_rejected"}))
        else:
            await websocket.send(json.dumps({"type": "error", "msg": "Unknown"}))

    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass
    except Exception as e:
        log.error(f"handler: {e}")


async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port,
                                max_size=10 * 1024 * 1024,
                                ping_interval=30, ping_timeout=60):
        log.info(f"VIPEN Relay  port={port}")
        await asyncio.Future()


asyncio.run(main())
