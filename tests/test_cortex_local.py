# tests/test_cortex_local.py
import asyncio
import orjson
import websockets
import ssl

async def test_emotiv_cortex_handshake():
    """
    Suite de prueba local para auditar la salud del Cortex Service de EMOTIV.
    Verifica de forma aislada que el WebSocket local responda en el puerto 6868.
    """
    uri = "wss://localhost:6868"
    print(f"[TEST] Iniciando diagnóstico perimetral en la dirección: {uri}")
    print("[TEST] Verificando presencia del Cortex Service local de EMOTIV...")

    # Forzamos un contexto SSL que ignore certificados auto-firmados comunes en el SDK de desarrollo local
    ssl_context = ssl._create_unverified_context()

    try:
        async with websockets.connect(uri, ssl=ssl_context, open_timeout=3.0) as ws:
            print("[+] [TEST ÉXITO] Scriptor conectado al socket de Cortex con éxito en el puerto 6868.")
            
            # Solicitud JSON-RPC básica para comprobar que el parser interno responde
            ping_rpc = {
                "jsonrpc": "2.0",
                "method": "getCortexInfo",
                "id": 101
            }
            
            print("[TEST] Enviando payload de verificación JSON-RPC...")
            await ws.send(orjson.dumps(ping_rpc))
            
            response = await ws.recv()
            data = orjson.loads(response)
            
            if "result" in data:
                version = data["result"].get("version", "Desconocida")
                build = data["result"].get("build", "Desconocida")
                print(f"[+] [TEST ÉXITO] Respuesta del SDK recibida de forma íntegra.")
                print(f"    -> Cortex Versión API: {version}")
                print(f"    -> Build de compilación: {build}")
                print("[+] [ESTADO] Entorno local operativo para la adquisición del EPOC Flex 2.")
            else:
                print(f"[-] [TEST ALERTA] Respuesta inesperada del SDK de Cortex: {data}")

    except ConnectionRefusedError:
        print("\n[-] [TEST CRÍTICO] Conexión rechazada en 'localhost:6868'.")
        print("    -> Razón: El servicio de EMOTIV (EmotivApp / Cortex Service) no está corriendo en segundo plano.")
        print("    -> Acción: Por favor, abre el software oficial de EMOTIV en tu máquina local e intenta de nuevo.")
    except asyncio.TimeoutError:
        print("\n[-] [TEST ERROR] Tiempo de espera de conexión agotado (Timeout de 3 segundos).")
        print("    -> Razón: El puerto responde pero el handshake criptográfico SSL está bloqueado o tardío.")
    except Exception as e:
        print(f"\n[-] [TEST FALLO] Ocurrió una anomalía inesperada durante el diagnóstico local: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(test_emotiv_cortex_handshake())
    except KeyboardInterrupt:
        print("\n[TEST] Diagnóstico local interrumpido por el operador.")
      
