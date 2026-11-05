# gema_orquestador/main.py
import argparse
import asyncio
import sys

# Importaciones nativas de los módulos internos de la gema
from gema_orquestador.daemon import V40MasterEcosystemDaemon
from gema_orquestador.network.client import V40RelayNodeClient

def main():
    """
    Punto de entrada unificado para la gema del asistente descentralizado.
    Orquesta el arranque del plano de control, plano de datos o la malla automatizada.
    """
    parser = argparse.ArgumentParser(
        description="Gema Orquestadora V40 - Infraestructura Descentralizada de Inferencia y Telemetría BCI"
    )
    
    # Definición de modos de operación
    subparsers = parser.add_subparsers(dest="mode", required=True, help="Modo de ejecución del nodo")

    # Subcomando: Master Daemon (Servidor central con Failover y DSP)
    server_parser = subparsers.add_parser("master", help="Inicia el V40 Master Ecosystem Daemon (Servidor)")
    server_parser.add_argument("--secret", type=str, default="v40_sovereign_ephemeral_key_2026", help="Clave secreta HMAC")
    server_parser.add_argument("--p-port", type=int, default=8888, help="Puerto Primario")
    server_parser.add_argument("--s-port", type=int, default=8889, help="Puerto Secundario")

    # Subcomando: Relay Client (Nodo periférico emisor individual / Puente Hardware)
    client_parser = subparsers.add_parser("relay", help="Inicia el V40 Relay Node Client o el puente de hardware")
    client_parser.add_argument("--host", type=str, default="127.0.0.1", help="Dirección IP o URL del nodo Master")
    client_parser.add_argument("--secret", type=str, default="v40_sovereign_ephemeral_key_2026", help="Clave secreta HMAC")
    client_parser.add_argument("--p-port", type=int, default=8888, help="Puerto Primario del Master")
    client_parser.add_argument("--s-port", type=int, default=8889, help="Puerto Secundario del Master")
    # Argumentos de acoplamiento dedicados para el SDK de EMOTIV Cortex
    client_parser.add_argument("--cortex", action="store_true", help="Usa el puente dinámico de Cortex SDK local")
    client_parser.add_argument("--client-id", type=str, default="", help="EMOTIV Cortex Client ID")
    client_parser.add_argument("--client-secret", type=str, default="", help="EMOTIV Cortex Client Secret")

    # Subcomando: Mesh (Orquestación masiva y automatizada en paralelo)
    mesh_parser = subparsers.add_parser("mesh", help="Lanza de forma automatizada una malla de múltiples clientes en paralelo")
    mesh_parser.add_argument("--host", type=str, default="127.0.0.1", help="Dirección IP del nodo Master")
    mesh_parser.add_argument("--secret", type=str, default="v40_sovereign_ephemeral_key_2026", help="Clave secreta HMAC")
    mesh_parser.add_argument("--p-port", type=int, default=8888, help="Puerto Primario del Master")
    mesh_parser.add_argument("--s-port", type=int, default=8889, help="Puerto Secundario del Master")
    mesh_parser.add_argument("--nodes", type=int, default=5, help="Número de nodos de relevo concurrentes a lanzar")

    args = parser.parse_args()
    secret_bytes = args.secret.encode("utf-8")

    # Orquestación de lazo asíncrono según el subcomando invocado
    if args.mode == "master":
        daemon = V40MasterEcosystemDaemon(
            secret_key=secret_bytes, 
            primary_port=args.p_port, 
            secondary_port=args.s_port
        )
        try:
            asyncio.run(daemon.run())
        except (KeyboardInterrupt, SystemExit):
            print("\n[V40 ORQUESTADOR] Cierre del Master Daemon ordenado por el operador.")
            
    elif args.mode == "relay":
        # Discriminación analítica: ¿Es simulación pura o adquisición directa desde Cortex SDK?
        if getattr(args, "cortex", False):
            from gema_orquestador.network.cortex_bridge import V40CortexBridgeClient
            client = V40CortexBridgeClient(
                client_id=args.client_id,
                client_secret=args.client_secret.encode('utf-8'),
                render_host=args.host,
                render_port=args.p_port,
                secret_key=secret_bytes
            )
            try:
                asyncio.run(client.stream_to_render())
            except KeyboardInterrupt:
                print("\n[V40 ORQUESTADOR] Cierre del puente de Cortex ordenado por el operador.")
        else:
            client = V40RelayNodeClient(
                host=args.host,
                primary_port=args.p_port,
                secondary_port=args.s_port,
                secret_key=secret_bytes
            )
            try:
                asyncio.run(client.run())
            except KeyboardInterrupt:
                print("\n[V40 ORQUESTADOR] Cierre del Relay Client ordenado por el operador.")

    elif args.mode == "mesh":
        from gema_orquestador.interface.instrumenter import V40EcosystemInstrumenter
        instrumenter = V40EcosystemInstrumenter(
            host=args.host,
            primary_port=args.p_port,
            secondary_port=args.s_port,
            secret_key=secret_bytes
        )
        try:
            asyncio.run(instrumenter.orchestrate_mesh(num_nodes=args.nodes))
        except KeyboardInterrupt:
            print("\n[V40 ORQUESTADOR] Apagado controlado de la malla de simulación por el operador.")

if __name__ == "__main__":
    main()
        
