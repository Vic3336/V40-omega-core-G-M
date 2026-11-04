# gema_orquestador/network/cortex_bridge.py
import asyncio
import hmac
import hashlib
import time
import orjson
import lz4.frame
import websockets

class V40CortexBridgeClient:
    """
    Cliente puente asíncrono de baja latencia. Conecta con el maestro local de Cortex SDK 
    en el puerto 6868, extrae los 32 canales físicos y los transmite firmados con HMAC 
    y comprimidos con LZ4 hacia el nodo maestro remoto en Render.
    """
    def __init__(self, client_id: str, client_secret: bytes, render_host: str = "://onrender.com", render_port: int = 8888, secret_key: bytes = b""):
        self.cortex_uri = "wss://localhost:6868"
        self.client_id = client_id
        self.client_secret = client_secret
        self.render_host = render_host
        self.render_port = render_port
        self.secret_key = secret_key
        self.auth_token = None
        self.headset_id = None
        self.session_id = None
        self.seq = 0

    def _sign(self, body: bytes) -> bytes:
        # Firma exacta de 32 bytes requerida por tu demonio central para validación perimetral
        return hmac.new(self.secret_key, body, hashlib.sha256).digest()

    async def _rpc_call(self, ws, method: str, params: dict = None) -> dict:
        """Estructura llamadas RPC en formato JSON exigidas por la API de Cortex."""
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "id": int(time.time_ns() & 0xFFFF)
        }
        if params:
            payload["params"] = params
        await ws.send(orjson.dumps(payload))
        resp = await ws.recv()
        return orjson.loads(resp)

    async def authenticate_cortex(self, ws):
        """Orquesta el ciclo de autenticación y generación de tokens en Cortex."""
        print("[+] [CORTEX] Solicitando token de acceso...")
        # 1. Autenticación con ID y Secret
        auth_resp = await self._rpc_call(ws, "authorize", {
            "clientId": self.client_id,
            "clientSecret": self.client_secret.decode('utf-8')
        })
        self.auth_token = auth_resp["result"]["cortexToken"]
        
        # 2. Descubrimiento de hardware (Flex 2 / EPOC)
        headsets = await self._rpc_call(ws, "queryHeadsets")
        if not headsets.get("result"):
            raise RuntimeError("No se detectó ningún hardware EMOTIV conectado al sistema local.")
        self.headset_id = headsets["result"][0]["id"]
        print(f"[+] [CORTEX] Hardware detectado: {self.headset_id}")

        # 3. Creación de sesión activa de telemetría
        session_resp = await self._rpc_call(ws, "createSession", {
            "cortexToken": self.auth_token,
            "status": "active",
            "headset": self.headset_id
        })
        self.session_id = session_resp["result"]["id"]
        print(f"[+] [CORTEX] Sesión de telemetría establecida de forma segura. ID: {self.session_id}")

    async def stream_to_render(self):
        """Bucle continuo de ingesta local y transmisión atómica a la nube."""
        print(f"[+] [PUENTE V40] Conectando de extremo a extremo al nodo maestro en Render...")
        
        # Deshabilitamos la verificación SSL estricta de websockets porque el daemon local de EMOTIV usa certificados self-signed
        async with websockets.connect(self.cortex_uri, ssl=False) as cortex_ws:
            await self.authenticate_cortex(cortex_ws)
            
            # Suscripción al flujo de datos crudos (EEG)
            await self._rpc_call(cortex_ws, "subscribe", {
                "cortexToken": self.auth_token,
                "session": self.session_id,
                "streams": ["eeg"]
            })
            print("[+] [PUENTE V40] Suscripción a canales Raw EEG exitosa. Iniciando lazo asíncrono.")

            # Conexión persistente contra el socket TCP del servidor central en Render
            render_reader, render_writer = await asyncio.open_connection(self.render_host, self.render_port)
            print("[+] [PUENTE V40] Túnel TCP establecido con el servidor central en Render.")

            try:
                async for msg in cortex_ws:
                    packet = orjson.loads(msg)
                    if "eeg" in packet:
                        # Estructura del stream de Cortex: [counter, [EEG_CHANNELS], markers]
                        raw_data = packet["eeg"]
                        channels = raw_data[1]  # Extrae la lista de microvoltios de los canales
                        
                        # Corrección dimensional estricta a 32 canales para NumPy en el daemon maestro
                        if len(channels) < 32:
                            channels.extend([0.0] * (32 - len(channels)))
                        elif len(channels) > 32:
                            channels = channels[:32]

                        self.seq += 1
                        payload = {
                            "seq": self.seq,
                            "timestamp": time.time_ns(),  # Reloj estricto en nanosegundos nativos
                            "channels": channels
                        }

                        # Compresión LZ4, empaquetado y firmado HMAC
                        body = lz4.frame.compress(orjson.dumps(payload))
                        header = self._sign(body)
                        length_bytes = len(body).to_bytes(4, 'big')

                        # Transmisión atómica no bloqueante por el socket TCP remoto
                        render_writer.write(header + length_bytes + body)
                        await render_writer.drain()

                        # Recepción del ACK del servidor maestro
                        resp_len_bytes = await render_reader.readexactly(4)
                        resp_len = int.from_bytes(resp_len_bytes, 'big')
                        await render_reader.readexactly(resp_len)

            except (asyncio.IncompleteReadError, ConnectionResetError, websockets.exceptions.ConnectionClosed):
                print("[-] [PUENTE ADVERTENCIA] Canal interrumpido. Realizando desconexión controlada...")
            finally:
                render_writer.close()
                try:
                    await render_writer.wait_closed()
                except Exception:
                    pass
          
