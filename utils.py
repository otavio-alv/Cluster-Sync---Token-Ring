import os
import time

CLUSTER_SIZE  = 5   # Nós do Cluster Sync
STORE_SIZE    = 3   # Nós do Cluster Store
TIMEOUT_S     = 3   # Timeout de conexão (segundos)
PING_INTERVAL = 2   # Intervalo entre PINGs de heartbeat (segundos)

def get_timestamp() -> int:
    """Retorna timestamp atual em milissegundos."""
    return int(time.time() * 1000)

def get_env_var(key: str, default_val: str) -> str:
    """Lê variável de ambiente com fallback para valor padrão."""
    return os.environ.get(key, default_val)
