"""
node.py -- No do Cluster Sync (Token Ring + Tolerancia a Falhas -- Opcao 1)

Portas:
  8080 -- atendimento de clientes
  9090 -- anel de tokens
  9092 -- health-check PING/PONG
"""

import os, sys, socket, threading, time, random, json
from queue import Queue, Empty
from utils     import CLUSTER_SIZE, STORE_SIZE, TIMEOUT_S, PING_INTERVAL, get_timestamp, get_env_var
from utils_log import log, separador


# ==========================================================================
# Estado global
# ==========================================================================
fila_pedidos: Queue = Queue()
lock         = threading.Lock()
token: list  = [{"node_id": i, "timestamp": -1, "client_id": -1}
                for i in range(CLUSTER_SIZE)]

MEU_ID:       int = -1
PROXIMO_ID:   int = -1
NODE_HOSTS        = [f"node{i}" for i in range(CLUSTER_SIZE)]
STORE_HOSTS       = [f"store{i}" for i in range(STORE_SIZE)]

alive: dict = {i: True for i in range(CLUSTER_SIZE)}
alive_lock  = threading.Lock()

_met = {
    "token_circulacoes":       0,
    "cs_acessos":              0,
    "store_latencia_total_ms": 0,
    "store_falhas":            0,
    "store_retries":           0,
    "pedidos_recebidos":       0,
    "committed_enviados":      0,
    "falhas_detectadas":       0,
    "failovers_aplicados":     0,
    "tokens_pulados":          0,
}
_met_lock = threading.Lock()


def L(nivel: str, msg: str) -> None:
    log(f"NO {MEU_ID}", nivel, msg, tipo="node")


# ==========================================================================
# Helpers de socket
# ==========================================================================

def socket_ainda_vivo(sock) -> bool:
    """
    Testa se o socket do cliente ainda esta conectado sem bloquear.
    Usa MSG_PEEK + DONTWAIT: se retornar 0 bytes, o cliente fechou.
    """
    if sock is None:
        return False
    try:
        data = sock.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT) # type: ignore
        return len(data) > 0   # 0 bytes = FIN recebido (cliente fechou)
    except BlockingIOError:
        return True            # nada para ler, mas conexao viva
    except OSError:
        return False           # socket morto


# ==========================================================================
# Acesso ao Cluster Store
# ==========================================================================

def enviar_para_store(c_id: int, c_timestamp: int) -> bool:
    ordem   = list(range(STORE_SIZE))
    random.shuffle(ordem)
    payload = json.dumps({
        "sync_node_id":     MEU_ID,
        "client_id":        c_id,
        "client_timestamp": c_timestamp,
        "write_timestamp":  get_timestamp(),
    }).encode()

    for tentativa, idx in enumerate(ordem):
        if tentativa > 0:
            with _met_lock:
                _met["store_retries"] += 1
        host = STORE_HOSTS[idx]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(TIMEOUT_S)
            s.connect((host, 7070))
            s.sendall(payload)
            s.shutdown(socket.SHUT_WR)
            resp = s.recv(16)
            s.close()
            if resp in (b"ACK", b"ACK\n"):
                L("OK", f"Escrita confirmada por {host} (cliente={c_id})")
                return True
        except OSError:
            L("WARN", f"Store {host} indisponivel -- tentando proximo...")

    with _met_lock:
        _met["store_falhas"] += 1
    L("ERRO", f"Nenhum store disponivel para cliente={c_id}")
    return False


def acessar_recurso(c_id: int, c_timestamp: int) -> None:
    separador(f"SECAO CRITICA -- No {MEU_ID} -- cliente={c_id}", tipo="node")
    t0  = get_timestamp()
    ok  = enviar_para_store(c_id, c_timestamp)
    lat = get_timestamp() - t0
    with _met_lock:
        _met["cs_acessos"]              += 1
        _met["store_latencia_total_ms"] += lat
    L("CS", f"Concluido | latencia={lat}ms | status={'OK' if ok else 'FALHA'}")


# ==========================================================================
# Rede / Token
# ==========================================================================

def recv_all(conn: socket.socket) -> bytes:
    data = b""
    while True:
        chunk = conn.recv(4096)
        if not chunk:
            break
        data += chunk
    return data


def proximo_vivo_a_partir(origem_id: int) -> int:
    """Retorna o proximo no vivo no anel a partir de origem_id (exclusive)."""
    for delta in range(1, CLUSTER_SIZE):
        candidato = (origem_id + delta) % CLUSTER_SIZE
        if candidato == MEU_ID:
            continue
        with alive_lock:
            if alive.get(candidato, True):
                return candidato
    return MEU_ID  # unico vivo


def enviar_token(dest_id: int, token_data: list) -> bool:
    host    = NODE_HOSTS[dest_id]
    payload = json.dumps(token_data).encode()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT_S + 1)
        s.connect((host, 9090))
        s.sendall(payload)
        s.close()
        return True
    except OSError:
        return False


def encaminhar_token_pulando_mortos(token_data: list) -> None:
    """
    Tenta enviar o token para o proximo no vivo.
    Se falhar, marca morto e avanca -- sempre a partir do no que falhou,
    nunca voltando ao inicio estatico do anel.
    """
    destino   = proximo_vivo_a_partir(MEU_ID)
    tentativas = 0

    while destino != MEU_ID and tentativas < CLUSTER_SIZE:
        tentativas += 1
        if enviar_token(destino, token_data):
            return

        separador(tipo="node")
        L("FALHA", f"No {destino} nao respondeu ao token -- pulando para proximo vivo")
        with alive_lock:
            alive[destino] = False
        with _met_lock:
            _met["falhas_detectadas"] += 1
            _met["tokens_pulados"]    += 1

        entry = token_data[destino]
        if entry["timestamp"] != -1:
            L("FALHA",
              f"No {destino} tinha pedido de cliente={entry['client_id']} "
              f"-- removendo do token (cenario 1.2/1.3)")
            token_data[destino]["timestamp"] = -1
            token_data[destino]["client_id"]  = -1
            with _met_lock:
                _met["failovers_aplicados"] += 1
        separador(tipo="node")

        proximo = proximo_vivo_a_partir(destino)
        if proximo == destino:
            break
        destino = proximo

    L("WARN", "Sou o unico no vivo -- token retido")


# ==========================================================================
# Thread: Anel (porta 9090)
# ==========================================================================

def thread_anel() -> None:
    global token
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("", 9090))
    server.listen(10)

    if MEU_ID == 0:
        L("INFO", "Aguardando cluster inicializar (12s)...")
        time.sleep(12)
        token = [{"node_id": i, "timestamp": -1, "client_id": -1}
                 for i in range(CLUSTER_SIZE)]
        encaminhar_token_pulando_mortos(token)

    # pedido_atual guarda o socket e metadados do cliente em espera
    pedido_atual: dict = {"socket": None, "client_id": -1}

    while True:
        conn, _ = server.accept()
        data     = recv_all(conn)
        conn.close()
        token = json.loads(data.decode())

        with _met_lock:
            _met["token_circulacoes"] += 1

        separador(f"NÓ {MEU_ID} VERIFICANDO O TOKEN", tipo="node")
        for entry in token:
            cid = entry['client_id'] if entry['client_id'] != -1 else "NULL"
            ts  = entry['timestamp']  if entry['timestamp']  != -1 else "NULL"
            L("TOKEN", f"NO [{entry['node_id']}]  |  CLIENT_ID = {str(cid):<6}  |  TIMESTAMP = {ts}")

        with lock:
            # ── NOVO: verifica se o cliente que estamos segurando ainda esta vivo ──
            if pedido_atual["socket"] is not None:
                if not socket_ainda_vivo(pedido_atual["socket"]):
                    L("WARN",
                      f"Cliente={pedido_atual['client_id']} fechou conexao "
                      f"-- descartando pedido do token")
                    # Remove entrada do token e libera o slot
                    token[MEU_ID]["timestamp"] = -1
                    token[MEU_ID]["client_id"]  = -1
                    try:
                        pedido_atual["socket"].close()
                    except OSError:
                        pass
                    pedido_atual = {"socket": None, "client_id": -1}

            # Fase 1: sou o mais prioritario com pedido ativo?
            if token[MEU_ID]["timestamp"] != -1 and pedido_atual["socket"] is not None:
                sou_o_menor = True
                for i in range(CLUSTER_SIZE):
                    t_i  = token[i]["timestamp"]
                    t_eu = token[MEU_ID]["timestamp"]
                    if t_i != -1 and (t_i < t_eu or (t_i == t_eu and i < MEU_ID)):
                        sou_o_menor = False
                        break

                if sou_o_menor:
                    acessar_recurso(pedido_atual["client_id"], get_timestamp())
                    try:
                        pedido_atual["socket"].sendall(b"COMMITTED")
                        pedido_atual["socket"].close()
                        with _met_lock:
                            _met["committed_enviados"] += 1
                        L("OK", f"COMMITTED -> cliente={pedido_atual['client_id']}")
                    except OSError:
                        L("WARN", f"Falha ao enviar COMMITTED para cliente={pedido_atual['client_id']} -- conexao perdida")
                    token[MEU_ID]["timestamp"] = -1
                    token[MEU_ID]["client_id"]  = -1
                    pedido_atual = {"socket": None, "client_id": -1}

            # Fase 2: inserir novo pedido da fila se slot livre
            if token[MEU_ID]["timestamp"] == -1:
                try:
                    r = fila_pedidos.get_nowait()
                    token[MEU_ID]["timestamp"] = r["timestamp"]
                    token[MEU_ID]["client_id"]  = r["client_id"]
                    pedido_atual = {"socket": r["socket"], "client_id": r["client_id"]}
                    L("INFO", f"Pedido do cliente={r['client_id']} inserido no token")
                except Empty:
                    pass

        time.sleep(1)
        encaminhar_token_pulando_mortos(token)


# ==========================================================================
# Thread: Clientes (porta 8080)
# ==========================================================================

def thread_cliente() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("", 8080))
    server.listen(10)
    while True:
        cli, _ = server.accept()
        data   = cli.recv(1024).decode().strip()
        if "|" in data:
            parts = data.split("|")
            cid   = int(parts[0])
            ts    = int(parts[1])
            with lock:
                fila_pedidos.put({"socket": cli, "client_id": cid, "timestamp": ts})
                with _met_lock:
                    _met["pedidos_recebidos"] += 1
            L("INFO", f"Novo pedido -- cliente={cid}")


# ==========================================================================
# Thread: Ping server (porta 9092)
# ==========================================================================

def thread_ping_server() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("", 9092))
    srv.listen(10)
    while True:
        conn, _ = srv.accept()
        try:
            conn.recv(16)
            conn.sendall(b"PONG")
        except OSError:
            pass
        finally:
            conn.close()


def ping_node(node_id: int) -> bool:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT_S)
        s.connect((NODE_HOSTS[node_id], 9092))
        s.sendall(b"PING")
        resp = s.recv(16)
        s.close()
        return resp == b"PONG"
    except OSError:
        return False


# ==========================================================================
# Thread: Heartbeat
# ==========================================================================

def thread_heartbeat() -> None:
    time.sleep(10)
    falhas: dict = {}
    while True:
        time.sleep(PING_INTERVAL)
        for i in range(CLUSTER_SIZE):
            if i == MEU_ID:
                continue
            vivo = ping_node(i)
            with alive_lock:
                era_vivo = alive.get(i, True)
                alive[i] = vivo
            if not vivo:
                falhas[i] = falhas.get(i, 0) + 1
                if falhas[i] == 2 and era_vivo:
                    separador(tipo="node")
                    L("FALHA", f">>> No {i} CONFIRMADO MORTO (heartbeat falhou 2x) <<<")
                    separador(tipo="node")
                    with _met_lock:
                        _met["falhas_detectadas"] += 1
            else:
                if not era_vivo:
                    separador(tipo="node")
                    L("OK", f">>> No {i} voltou ao ar -- reintegrado ao anel <<<")
                    separador(tipo="node")
                    with alive_lock:
                        alive[i] = True
                falhas.pop(i, None)


# ==========================================================================
# Thread: Metricas
# ==========================================================================

def thread_metrics() -> None:
    while True:
        time.sleep(15)
        with _met_lock:
            m = _met.copy()
        cs  = m["cs_acessos"]
        lat = (m["store_latencia_total_ms"] / cs) if cs > 0 else 0
        with alive_lock:
            vivos = [i for i in range(CLUSTER_SIZE) if alive.get(i, True) or i == MEU_ID]

        separador(f"METRICAS -- No {MEU_ID}  |  nos vivos={vivos}", tipo="node")
        rows = [
            ("Circulacoes do token",      m["token_circulacoes"]),
            ("Acessos a secao critica",   cs),
            ("Latencia media store (ms)", f"{lat:.1f}"),
            ("Falhas no store",           m["store_falhas"]),
            ("Retries no store",          m["store_retries"]),
            ("Pedidos recebidos",         m["pedidos_recebidos"]),
            ("COMMITTEDs enviados",       m["committed_enviados"]),
            ("Falhas detectadas",         m["falhas_detectadas"]),
            ("Failovers aplicados",       m["failovers_aplicados"]),
            ("Tokens pulados (mortos)",   m["tokens_pulados"]),
        ]
        for label, valor in rows:
            log(f"NO {MEU_ID}", "METRICA", f"{label:<34} {valor}", tipo="node")


# ==========================================================================
# Entrypoint
# ==========================================================================

if __name__ == "__main__":
    MEU_ID     = int(get_env_var("MY_ID",          "0"))
    PROXIMO_ID = int(get_env_var("NEXT_NODE_ID",   str((MEU_ID + 1) % CLUSTER_SIZE)))
    separador(f"INICIANDO NO {MEU_ID}  |  proximo=node{PROXIMO_ID}", tipo="node")

    threads = [
        threading.Thread(target=thread_cliente,     daemon=True, name="cliente"),
        threading.Thread(target=thread_anel,        daemon=True, name="anel"),
        threading.Thread(target=thread_ping_server, daemon=True, name="ping-srv"),
        threading.Thread(target=thread_heartbeat,   daemon=True, name="heartbeat"),
        threading.Thread(target=thread_metrics,     daemon=True, name="metrics"),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
