"""
client.py -- Cliente do Cluster Sync

Envia entre 10 e 50 pedidos ao recurso R.

Comportamento de tolerancia a falhas:
  - Se o no-alvo nao responder por MAX_FALHAS_CONSECUTIVAS tentativas seguidas,
    entra em modo AGUARDANDO com intervalo crescente (backoff).
  - Continua tentando indefinidamente ate o no voltar.
  - Quando o no responder novamente, retoma de onde parou.
"""

import socket
import time
import random
import sys

from utils     import get_timestamp, get_env_var
from utils_log import log

MAX_FALHAS_CONSECUTIVAS = 5    # tentativas antes de entrar em modo espera
ESPERA_RECONEXAO_MIN    = 5    # segundos iniciais de espera entre tentativas de reconexao
ESPERA_RECONEXAO_MAX    = 30   # teto do backoff


def L(nivel, msg, cid):
    log(f"CLI {cid}", nivel, msg, tipo="node")


def tentar_conectar(host, port, timeout=10):
    """Tenta abrir conexao TCP. Retorna socket ou None."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        return sock
    except OSError:
        return None


def main():
    random.seed()

    MEU_ID    = int(get_env_var("CLIENT_ID",        "100"))
    NODE_HOST =     get_env_var("TARGET_NODE_HOST", "127.0.0.1")
    NODE_PORT = int(get_env_var("TARGET_NODE_PORT", "8080"))

    total = 9 + random.randint(1, 41)
    L("INFO", f"Iniciado -- {total} acessos planejados -> no {NODE_HOST}", MEU_ID)

    latencias           = []
    falhas_consecutivas = 0
    espera_reconexao    = ESPERA_RECONEXAO_MIN
    i = 1

    while i <= total:

        # ── Modo AGUARDANDO: no estava morto, tenta reconectar ──────────────
        if falhas_consecutivas >= MAX_FALHAS_CONSECUTIVAS:
            L("WARN",
              f"No {NODE_HOST} indisponivel -- aguardando {espera_reconexao}s "
              f"para tentar reconexao...",
              MEU_ID)
            time.sleep(espera_reconexao)

            # Backoff exponencial com teto
            espera_reconexao = min(espera_reconexao * 2, ESPERA_RECONEXAO_MAX)

            # Testa se o no voltou com um ping TCP simples
            sock_teste = tentar_conectar(NODE_HOST, NODE_PORT, timeout=5)
            if sock_teste is None:
                continue   # ainda morto, continua aguardando
            # Voltou! Fecha o socket de teste e retoma normalmente
            sock_teste.close()
            L("OK",
              f"No {NODE_HOST} voltou ao ar -- retomando do acesso {i}/{total}",
              MEU_ID)
            falhas_consecutivas = 0
            espera_reconexao    = ESPERA_RECONEXAO_MIN
            continue

        # ── Acesso normal ────────────────────────────────────────────────────
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(40)
            sock.connect((NODE_HOST, NODE_PORT))

            ts = get_timestamp()
            sock.sendall(f"{MEU_ID}|{ts}".encode())

            t0       = get_timestamp()
            response = sock.recv(1024).decode().strip()
            latencia = get_timestamp() - t0
            sock.close()

            if response == "COMMITTED":
                latencias.append(latencia)
                media  = sum(latencias) / len(latencias)
                blocos = min(20, latencia // 100)
                barra  = "|" * blocos + "." * (20 - blocos)
                L("OK",
                  f"[{i:>2}/{total}] COMMITTED  lat={latencia}ms  "
                  f"med={media:.0f}ms  [{barra}]",
                  MEU_ID)
                falhas_consecutivas = 0
                espera_reconexao    = ESPERA_RECONEXAO_MIN
                i += 1
            else:
                L("WARN", f"Resposta inesperada: \"{response}\"", MEU_ID)
                falhas_consecutivas += 1

        except OSError as e:
            falhas_consecutivas += 1
            L("FALHA",
              f"No {NODE_HOST} indisponivel [{falhas_consecutivas}/"
              f"{MAX_FALHAS_CONSECUTIVAS}]: {e}",
              MEU_ID)

        espera = 2 + random.randint(0, 3)
        L("ESPERA", f"Aguardando {espera}s...", MEU_ID)
        time.sleep(espera)

    media_final = sum(latencias) / len(latencias) if latencias else 0
    L("INFO",
      f"Finalizado -- {total} acessos concluidos | "
      f"latencia media={media_final:.0f}ms",
      MEU_ID)


if __name__ == "__main__":
    main()
