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
    Orquesta el arranque del plano de control o del plano de datos según los argumentos de consola.
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

    # Subcomando: Relay Client (Nodo periférico emisor)
    client_parser = subparsers.add_parser("relay", help="Inicia el V40 Relay Node Client (Emisor simulado)")
    client_parser.add_argument("--host", type=str, default="127.0.0.1", help="Dirección IP del nodo Master")
    client_parser.add_argument("--secret", type=str, default="v40_sovereign_ephemeral_key_2026", help="Clave secreta HMAC")
    client_parser.add_argument("--p-port", type=int, default=8888, help="Puerto Primario del Master")
    client_parser.add_argument("--s-port", type=int, default=8889, help="Puerto Secundario del Master")

    args = parser.parse_args()
    secret_bytes = args.secret.encode("utf-8")

    # Orquestación utilizando guiones bajos por la conversión de argparse
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

if __name__ == "__main__":
    main()
  
