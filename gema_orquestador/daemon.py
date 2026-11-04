# gema_orquestador/daemon.py
import asyncio
import time
import orjson
import lz4.frame
import hmac
import hashlib
import numpy as np
import os
import signal
from collections import deque

try:
    import serial
    SERIAL_AVAILABLE = True
except ImportError:
    SERIAL_AVAILABLE = False

class V40CircularStoreBuffer:
    """
    V40 Circular Store Buffer (V40-Woofer / V40MasterRing)
    Mantiene un búfer circular de tamaño fijo y utiliza un asyncio.Lock para 
    serializar de forma segura las escrituras a disco sin bloquear el bucle de eventos.
    """
    def __init__(self, max_packets: int = 1500, filepath: str = "v40_master_ring.bin"):
        self.max_packets = max_packets
        self.filepath = filepath
        self.buffer = deque(maxlen=max_packets)
        self.write_lock = asyncio.Lock()
        
    def push_and_flush(self, packet_bytes: bytes):
        if not packet_bytes:
            return
        self.buffer.append(packet_bytes)
        asyncio.create_task(self._async_flush())

    async def _async_flush(self):
        async with self.write_lock:
            def _write_disk():
                try:
                    with open(self.filepath, "wb") as f:
                        for block in self.buffer:
                            f.write(len(block).to_bytes(4, 'big') + block)
                except Exception as e:
                    print(f"[ALMACÉN ERROR] Fallo al volcar búfer a disco: {e}")
            
            await asyncio.to_thread(_write_disk)

class V40AdaptiveDSP:
    """
    Adaptive Digital Signal Processing (DSP) core utilizando NumPy 
    para detección de anomalías mediante Z-score en streaming en tiempo real.
    """
    def __init__(self, window_size: int = 40):
        self.window_size = window_size
        self.history = deque(maxlen=window_size)

    def analyze(self, channels: list) -> tuple[bool, float]:
        arr = np.array(channels, dtype=np.float32)
        self.history.append(arr)
        
        if len(self.history) < self.window_size:
            return True, 0.0
            
        stack = np.array(self.history)
        mean_val = np.mean(stack, axis=0)
        std_val = np.std(stack, axis=0)
        
        z_scores = np.abs((arr - mean_val) / (std_val + 1e-6))
        max_z = float(np.max(z_scores))
        return max_z <= 3.0, max_z

class V40SerialActuatorBridge:
    """
    Serial Actuator Bridge vía PySerial implementando empaquetado directo (0xFF / 0xFE).
    """
    def __init__(self, port: str = "/dev/ttyUSB0", baudrate: int = 115200):
        self.serial_conn = None
        self.connected = False
        if SERIAL_AVAILABLE:
            try:
                self.serial_conn = serial.Serial(port, baudrate, timeout=0.1)
                self.connected = True
                print(f"[HARDWARE] Conectado al bus serial en {port}")
            except Exception as e:
                print(f"[HARDWARE ADVERTENCIA] Modo simulación activo ({e}).")

    def dispatch(self, action_code: int, value: float):
        if not self.connected or not self.serial_conn:
            return
        try:
            packet = bytearray([0xFF, action_code])
            packet.extend(np.float32(value).tobytes())
            packet.append(0xFE)
            self.serial_conn.write(packet)
            self.serial_conn.flush()
        except Exception as e:
            print(f"[HARDWARE ERROR] Fallo de transmisión serial: {e}")

    def close(self):
        if self.connected and self.serial_conn and self.serial_conn.is_open:
            self.serial_conn.close()
            print("[HARDWARE] Puerto serial cerrado correctamente.")

class V40MasterEcosystemDaemon:
    """
    Master Ecosystem Daemon optimizado con failover, verificación HMAC, 
    protección de concurrencia en disco y manejo seguro de señales.
    """
    def __init__(self, secret_key: bytes, primary_port: int = 8888, secondary_port: int = 8889):
        self.secret_key = secret_key
        self.primary_port = primary_port
        self.secondary_port = secondary_port
        self.ring_store = V40CircularStoreBuffer()
        self.dsp = V40AdaptiveDSP()
        self.actuator = V40SerialActuatorBridge()
        self.active_channel = "PRIMARY"
        self.primary_latencies = []
        self.servers = []
        self.shutdown_event = asyncio.Event()

    def _verify(self, data: bytes, sig: bytes) -> bool:
        expected = hmac.new(self.secret_key, data, hashlib.sha256).digest()
        return hmac.compare_digest(expected, sig)

    async def handle_stream(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, port_name: str):
        peer = writer.get_extra_info('peername')
        print(f"[CONEXIÓN] Canal [{port_name}] establecido con {peer}")
        try:
            while not self.shutdown_event.is_set():
                try:
                    header = await asyncio.wait_for(reader.readexactly(32), timeout=1.0)
                    length_bytes = await reader.readexactly(4)
                    length = int.from_bytes(length_bytes, 'big')
                    body = await reader.readexactly(length)
                except asyncio.TimeoutError:
                    continue

                t_start = time.perf_counter_ns()

                if not self._verify(body, header):
                    print(f"[SEGURIDAD ALERTA] HMAC no válido en {port_name}. Trama rechazada.")
                    break

                self.ring_store.push_and_flush(body)

                packet = orjson.loads(lz4.frame.decompress(body))
                seq = packet.get("seq", 0)
                channels = packet.get("channels", [])

                is_normal, max_z = self.dsp.analyze(channels)

                if not is_normal:
                    self.actuator.dispatch(0x01, max_z)
                    state_msg = f"ANOMALÍA (Z: {max_z:.2f}) -> Actuador Activado"
                else:
                    self.actuator.dispatch(0x00, float(np.mean(channels)))
                    state_msg = "DSP ÓPTIMO"

                t_end = time.perf_counter_ns()
                transit_ms = (t_end - t_start) / 1_000_000

                if port_name == "PRIMARY":
                    self.primary_latencies.append(transit_ms)
                    if len(self.primary_latencies) > 10:
                        self.primary_latencies.pop(0)
                    
                    if np.mean(self.primary_latencies) > 2.5 and self.active_channel == "PRIMARY":
                        self.active_channel = "SECONDARY"
                        print(f"[FAILOVER] ⚠️ Alta latencia en canal Primario. Conmutando tráfico a puerto Secundario.")

                print(f"[{port_name}] Seq: {seq} | Tránsito Interno: {transit_ms:.3f} ms | Ruta Activa: {self.active_channel} | {state_msg}")

                resp = lz4.frame.compress(orjson.dumps({"status": "ACK", "route": self.active_channel}))
                writer.write(len(resp).to_bytes(4, 'big') + resp)
                await writer.drain()

        except (asyncio.IncompleteReadError, ConnectionResetError):
            print(f"[DESCONEXIÓN] Canal {port_name} cerrado limpiamente por el cliente.")
        except Exception as e:
            print(f"[ERROR EN STREAM] Canal {port_name}: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def shutdown(self):
        print("\n[V40 MASTER] Deteniendo servidores y realizando cierre limpio (Systemd-compliant)...")
        self.shutdown_event.set()
        for srv in self.servers:
            srv.close()
            await srv.wait_closed()
        
        self.actuator.close()
        print("[V40 MASTER] Ecosistema apagado de forma segura. Reporte de salida exitoso: `veto_aprobado: True`")

    async def run(self):
        loop = asyncio.get_running_loop()
        
        if os.name == 'posix':
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        else:
            print("[INFO] Entorno no-POSIX detectado. El apagado se gestionará mediante captura de interrupción nativa.")

        server_prim = await asyncio.start_server(lambda r, w: self.handle_stream(r, w, "PRIMARY"), "0.0.0.0", self.primary_port)
        server_sec = await asyncio.start_server(lambda r, w: self.handle_stream(r, w, "SECONDARY"), "0.0.0.0", self.secondary_port)
        
        self.servers = [server_prim, server_sec]

        print(f"[V40 MASTER ECOSYSTEM] Núcleo operativo iniciado con Apagado Limpio.")
        print(f"  -> Puerto Primario   : {self.primary_port}")
        print(f"  -> Puerto Secundario : {self.secondary_port}")
        print(f"  -> Almacenamiento    : v40_master_ring.bin (Búfer Circular)")

        async with server_prim, server_sec:
            await asyncio.gather(
                server_prim.serve_forever(),
                server_sec.serve_forever(),
                return_exceptions=True
      )
  
