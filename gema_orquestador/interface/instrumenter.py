# gema_orquestador/interface/instrumenter.py
import asyncio
import sys
from gema_orquestador.network.client import V40RelayNodeClient

class V40EcosystemInstrumenter:
    """
    Orquestador de automatización encargado de levantar, supervisar y controlar
    múltiples instancias de clientes periféricos en paralelo dentro del lazo asíncrono.
    """
    def __init__(self, host: str = "127.0.0.1", primary_port: int = 8888, secondary_port: int = 8889, secret_key: bytes = b""):
        self.host = host
        self.primary_port = primary_port
        self.secondary_port = secondary_port
        self.secret_key = secret_key
        self.active_tasks = []

    async def launch_node(self, node_index: int):
        """Inicializa e invoca una instancia individual del cliente de relevo."""
        node_id = f"RELAY_INSTANCE_{node_index:02d}"
        print(f"[INSTRUMENTER] Instanciando de forma autónoma: {node_id}")
        
        # Instanciamos el cliente periférico acoplado
        client = V40RelayNodeClient(
            host=self.host,
            primary_port=self.primary_port,
            secondary_port=self.secondary_port,
            secret_key=self.secret_key
        )
        # Modificamos dinámicamente el identificador para la auditoría del servidor
        client.node_id = node_id
        
        try:
            await client.run()
        except asyncio.CancelledError:
            print(f"[INSTRUMENTER] Detención controlada para: {node_id}")
        except Exception as e:
            print(f"[INSTRUMENTER ERROR] Falla crítica en {node_id}: {e}")

    async def orchestrate_mesh(self, num_nodes: int = 5):
        """Lanza la malla descentralizada de clientes concurrentes en paralelo."""
        print(f"[INSTRUMENTER] Levantando malla de simulación con {num_nodes} nodos...")
        
        # Creamos las tareas asíncronas concurrentes para cada cliente
        for i in range(1, num_nodes + 1):
            task = asyncio.create_task(self.launch_node(i))
            self.active_tasks.append(task)
            # Pequeño delay de gracia para evitar saturación en el handshake inicial del socket
            await asyncio.sleep(0.2)

        try:
            # Mantiene el bucle de la malla corriendo de forma indefinida
            await asyncio.gather(*self.active_tasks)
        except asyncio.CancelledError:
            print("\n[INSTRUMENTER] Deteniendo todos los nodos activos de la malla...")
        finally:
            for task in self.active_tasks:
                if not task.done():
                    task.cancel()
                  
