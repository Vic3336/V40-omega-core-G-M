# Gema Orquestadora V40 (V40-Omega Core)

Infraestructura de software unificada, descentralizada y de alto rendimiento diseñada para la ingesta asíncrona, validación criptográfica perimetral y procesamiento digital en tiempo real de flujos de telemetría masivos y señales bioeléctricas de interfaces cerebro-computador (BCI/EEG).

## 🚀 Arquitectura del Ecosistema

La gema encapsula un sistema asíncrono robusto dividido en dos planos operativos de baja latencia operando sobre hardware bare-metal:

1. **Plano de Control (V40MasterEcosystemDaemon):** Administra sockets TCP concurrentes con un mecanismo dinámico de tolerancia a fallos (*dual-port failover relay* en puertos 8888/8889). Realiza validaciones estrictas de integridad, auditoría espectral inmediata y vuelca un búfer circular atómico no bloqueante a disco.
2. **Plano de Datos (V40RelayNodeClient):** Nodo periférico de transmisión de flujos binarios diseñado para empaquetar, comprimir dinámicamente con LZ4, firmar criptográficamente (HMAC-SHA256) y despachar matrices de 32 canales físicos a ~20 Hz con marcas de tiempo en nanosegundos nativos.

## 🛡️ Características Críticas de Rendimiento y Seguridad

* **Verificación Criptográfica Perimetral:** Todo paquete entrante es verificado en la frontera mediante firmas HMAC-SHA256 directas sobre el bloque binario comprimido, descartando intentos de inyección o corrupción antes de delegar recursos a la descompresión.
* **Persistencia Atómica No Bloqueante:** Implementa `V40CircularStoreBuffer` respaldado por un buffer circular de tamaño fijo (`collections.deque`) y un esquema asíncrono con `asyncio.Lock` y `asyncio.to_thread` para mitigar el *jitter* y caídas de rendimiento por bloqueo de E/S de disco.
* **Continuidad de Fase en DSP:** Filtrado digital vectorizado a una frecuencia de muestreo de 250 Hz empleando filtros Notch (60 Hz) y Pasa-banda Butterworth de 4to orden (0.5 Hz - 50 Hz). Las condiciones iniciales se precalculan mediante coeficientes de estado para procesar bloques fraccionados manteniendo estabilidad de fase matemática.
* **Tolerancia Activa a Fallos:** Si las latencias de tránsito en el puerto primario se degradan por encima del umbral estricto de 2.5 ms, la gema conmuta el tráfico dinámicamente al puerto secundario sin interrupciones del hilo.

## 📁 Estructura del Repositorio

```text
V40-omega-core-G-M/
├── pyproject.toml              # Especificaciones modernas de empaquetado (PEP 518/621)
├── README.md                   # Documentación técnica de la infraestructura
└── gema_orquestador/
    ├── __init__.py
    ├── main.py                 # Orquestador unificado y punto de entrada de la consola
    ├── daemon.py               # Servidor maestro central con Failover y DSP asíncrono
    └── network/
        ├── __init__.py
        └── client.py           # Cliente periférico de relevo y simulación end-to-end
```

## 🛠️ Instalación e Implementación Local

Para compilar e instalar la gema de manera nativa en tu entorno operativo de desarrollo o contenedores de producción, clona el repositorio y ejecuta la instalación en modo editable:

```bash
pip install -e .
```

Esto expondrá el comando global `gema-v40` directamente en las variables de entorno del sistema operativo.

## 💻 Guía de Operación en Consola (Pruebas End-to-End)

El ecosistema está diseñado para operarse y desplegarse mediante subcomandos explícitos pasados al orquestador unificado:

### 1. Inicializar el Demonio Maestro (Servidor Central)
Abre una terminal y levanta la infraestructura del nodo central en escucha de la malla descentralizada:
```bash
gema-v40 master --p-port 8888 --s-port 8889 --secret v40_sovereign_ephemeral_key_2026
```

### 2. Inicializar el Nodo de Relevo (Cliente Emisor)
Abre una segunda terminal independiente para inyectar las tramas binarias cifradas de 32 canales en tiempo real hacia el Master:
```bash
gema-v40 relay --host 127.0.0.1 --p-port 8888 --secret v40_sovereign_ephemeral_key_2026
```

## 🛑 Apagado Seguro (Graceful Shutdown)

El sistema integra manejadores de señales asíncronos nativos compatibles con entornos `systemd` para Linux/Unix. Al recibir una señal `SIGINT` o `SIGTERM`, la gema inicia una rutina de contención que suspende la escucha de sockets, vacía los estados remanentes de memoria, cierra el bus del actuador serial de forma segura e interrumpe la ejecución devolviendo un código de salida controlado.
