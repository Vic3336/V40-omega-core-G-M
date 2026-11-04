# gema_orquestador/network/client.py
import asyncio
import time
import orjson
import lz4.frame
import hmac
import hashlib
import random

class V40RelayNodeClient:
    """
    Cliente periférico asíncrono para el V40 Master Ecosystem Daemon.
    Empaqueta, comprueba criptográficamente y transmite flujos de datos en tiempo real (32 canales EEG)
    con soporte de conmutación automática entre puertos primario y secundario.
    """
    def __init__(self, host: str = "127.0.0.1", primary_port: int = 8888, secondary_port: int = 8889, secret_key: bytes = b""):
        self.host = host
        self.primary_port = primary_port
        self.secondary_port = secondary_port
        self.secret_key = secret_key
        self.active_port = primary_port
        self.seq = 0

    def _sign(self, body: bytes) -> bytes:
        return hmac.new(self.secret_key, body, hashlib.sha256).digest()

    async def run(self):
        print(f"[CLIENTE V40] Iniciando nodo periférico RelayNodeClient (Contrato: 32 Canales / Nanosegundos)...")
        while True:
            reader, writer = None, None
            
            try:
                print(f"[CLIENTE V40] Conectando a {self.host}:{self.active_port}...")
                reader, writer = await asyncio.open_connection(self.host, self.active_port)
                print(f"[CLIENTE V40] Conexión establecida con éxito en puerto {self.active_port}.")
            except Exception as e:
                print(f"[CLIENTE ADVERTENCIA] No se pudo conectar al puerto {self.active_port} ({e}). Alternando puerto...")
                self.active_port = self.secondary_port if self.active_port == self.primary_port else self.primary_port
                await asyncio.sleep(2.0)
                continue

            try:
                while True:
                    self.seq += 1
                    channels = [random.gauss(0.0, 1.2) for _ in range(32)]
                    
                    payload = {
                        "seq": self.seq,
                        "timestamp": time.time_ns(),
                        "channels": channels
                    }

                    body = lz4.frame.compress(orjson.dumps(payload))
                    header = self._sign(body)
                    length_bytes = len(body).to_bytes(4, 'big')

                    writer.write(header + length_bytes + body)
                    await writer.drain()

                    resp_len_bytes = await reader.readexactly(4)
                    resp_len = int.from_bytes(resp_len_bytes, 'big')
                    resp_body = await reader.readexactly(resp_len)
                    
                    ack_packet = orjson.loads(lz4.frame.decompress(resp_body))
                    print(f"[CLIENTE] Seq: {self.seq:04d} | Ruta Activa Servidor: {ack_packet.get('route')} | Estado: {ack_packet.get('status')}")

                    await asyncio.sleep(0.05)

            except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
                print(f"[CLIENTE ADVERTENCIA] Conexión perdida en el puerto {self.active_port}. Reintentando failover...")
                self.active_port = self.secondary_port if self.active_port == self.primary_port else self.primary_port
            except Exception as e:
                print(f"[CLIENTE ERROR] Error crítico en el ciclo de envío: {e}")
                await asyncio.sleep(1.0)
            finally:
                if writer:
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
          
