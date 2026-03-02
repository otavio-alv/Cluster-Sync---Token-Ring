"""
store.py -- No do Cluster Store (Protocolo 2: Copia Primaria Migratoria)

Portas:
  7070 -- WRITE do Cluster Sync
  7071 -- replicacao assincrona entre stores
  7072 -- PING health-check
  7073 -- TRANSFER de estado entre stores
"""

import os, sys, socket, threading, time, json
from utils     import get_timestamp, get_env_var, STORE_SIZE, TIMEOUT_S, PING_INTERVAL
from utils_log import log, separador

# ==========================================================================
# Estado global
# ==========================================================================
MEU_ID: int       = -1
_primary_id: int  = 0
_primary_lock     = threading.Lock()
_data_lock        = threading.Lock()
_file_lock        = threading.Lock()
_store_data: dict = {}

STORE_HOSTS = [f"store{i}" for i in range(STORE_SIZE)]

_met = {
    "writes_recebidos":       0,
    "writes_executados":      0,
    "replicacoes_async_ok":   0,
    "replicacoes_async_fail": 0,
    "transfers_como_origem":  0,
    "transfers_como_destino": 0,
    "failovers":              0,
    "pings_enviados":         0,
    "pings_falhos":           0,
}
_met_lock = threading.Lock()

def papel() -> str:
    return "PRI" if is_primary() else "BAK"

# Atalho: log para este store, sempre tipo "store"
def L(nivel: str, msg: str) -> None:
    log(f"STORE {MEU_ID}|{papel()}", nivel, msg, tipo="store")


# ==========================================================================
# Primario
# ==========================================================================

def get_primary() -> int:
    with _primary_lock:
        return _primary_id

def set_primary(new_id: int) -> None:
    global _primary_id
    with _primary_lock:
        old = _primary_id
        if old == new_id:
            return
        _primary_id = new_id
    novo_papel = "PRIMARIO" if new_id == MEU_ID else f"BACKUP (primario=store{new_id})"
    L("ELEICAO", f"store{old} --> store{new_id}  |  novo papel: {novo_papel}")

def is_primary() -> bool:
    return get_primary() == MEU_ID


# ==========================================================================
# Rede
# ==========================================================================

def _recv_all(conn: socket.socket) -> bytes:
    data = b""
    while True:
        chunk = conn.recv(4096)
        if not chunk:
            break
        data += chunk
    return data

def _connect(host: str, port: int) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(TIMEOUT_S)
    s.connect((host, port))
    return s

def ping_store(host: str) -> bool:
    try:
        s = _connect(host, 7072)
        s.sendall(b"PING")
        resp = s.recv(16)
        s.close()
        return resp == b"PONG"
    except OSError:
        return False


# ==========================================================================
# Dados
# ==========================================================================

def get_data_snapshot() -> dict:
    with _data_lock:
        return json.loads(json.dumps(_store_data))

def apply_record(record: dict) -> None:
    with _data_lock:
        _store_data.setdefault("records", []).append(record)
        _store_data["version"] = _store_data.get("version", 0) + 1
    os.makedirs("/app/data", exist_ok=True)
    with _file_lock:
        with open(f"/app/data/store{MEU_ID}_log.txt", "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
    with _met_lock:
        _met["writes_executados"] += 1

def load_from_snapshot(snapshot: dict) -> None:
    global _store_data
    with _data_lock:
        _store_data = snapshot
    os.makedirs("/app/data", exist_ok=True)
    with _file_lock:
        with open(f"/app/data/store{MEU_ID}_log.txt", "w", encoding="utf-8") as f:
            for rec in snapshot.get("records", []):
                f.write(json.dumps(rec) + "\n")


# ==========================================================================
# Protocolo 2
# ==========================================================================

def pedir_transfer_ao_primario(pid: int) -> bool:
    host = STORE_HOSTS[pid]
    try:
        s   = _connect(host, 7073)
        msg = json.dumps({"type": "REQUEST_TRANSFER", "novo_primario": MEU_ID}).encode()
        s.sendall(msg)
        s.shutdown(socket.SHUT_WR)
        resp = json.loads(_recv_all(s).decode())
        s.close()
        if resp.get("status") == "TRANSFERRED":
            load_from_snapshot(resp["snapshot"])
            L("TRANSFER", f"Dados recebidos de store{pid} | versao={resp['snapshot'].get('version',0)}")
            return True
        return False
    except OSError:
        return False

def propagar_backup_async(record: dict) -> None:
    def _replicar():
        for i, host in enumerate(STORE_HOSTS):
            if i == MEU_ID:
                continue
            try:
                s = _connect(host, 7071)
                s.sendall(json.dumps(record).encode())
                s.shutdown(socket.SHUT_WR)
                resp = s.recv(16)
                s.close()
                with _met_lock:
                    if resp == b"ACK":
                        _met["replicacoes_async_ok"] += 1
                    else:
                        _met["replicacoes_async_fail"] += 1
            except OSError:
                with _met_lock:
                    _met["replicacoes_async_fail"] += 1
                L("WARN", f"store{i} indisponivel na replicacao assincrona")
    threading.Thread(target=_replicar, daemon=True).start()

def notificar_novo_primario() -> None:
    msg = json.dumps({"type": "NEW_PRIMARY", "primary_id": MEU_ID}).encode()
    for i, host in enumerate(STORE_HOSTS):
        if i == MEU_ID:
            continue
        try:
            s = _connect(host, 7073)
            s.sendall(msg)
            s.shutdown(socket.SHUT_WR)
            s.recv(16)
            s.close()
        except OSError:
            pass

def executar_como_primario(payload: dict, conn: socket.socket) -> None:
    record = {
        "sync_node_id":     payload.get("sync_node_id"),
        "client_id":        payload.get("client_id"),
        "client_timestamp": payload.get("client_timestamp"),
        "write_timestamp":  get_timestamp(),
        "primary_store":    MEU_ID,
    }
    apply_record(record)
    with _data_lock:
        versao = _store_data.get("version", 0)
    L("WRITE", f"Escrita local | cliente={record['client_id']} | versao={versao}")
    try:
        conn.sendall(b"ACK")
    except OSError:
        pass
    finally:
        conn.close()
    propagar_backup_async(record)

def handle_write_request(conn: socket.socket) -> None:
    try:
        payload = json.loads(_recv_all(conn).decode())
    except (OSError, json.JSONDecodeError) as e:
        L("ERRO", f"Falha ao ler WRITE: {e}")
        conn.close()
        return

    with _met_lock:
        _met["writes_recebidos"] += 1

    pid = get_primary()
    if pid == MEU_ID:
        executar_como_primario(payload, conn)
        return

    L("TRANSFER", f"Nao sou primario -- solicitando transfer a store{pid}...")
    transferred = pedir_transfer_ao_primario(pid)

    if transferred:
        set_primary(MEU_ID)
        notificar_novo_primario()
        executar_como_primario(payload, conn)
    else:
        separador(tipo="store")
        L("FALHA", f"Transfer falhou (store{pid} indisponivel) -- assumindo primario por failover")
        separador(tipo="store")
        set_primary(MEU_ID)
        with _met_lock:
            _met["failovers"] += 1
        notificar_novo_primario()
        executar_como_primario(payload, conn)


# ==========================================================================
# Servidores
# ==========================================================================

def thread_write_server() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("", 7070)); srv.listen(10)
    L("INFO", "WRITE server porta 7070 pronto")
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=handle_write_request, args=(conn,), daemon=True).start()

def thread_replication_server() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("", 7071)); srv.listen(10)
    L("INFO", "REPL server porta 7071 pronto")
    while True:
        conn, _ = srv.accept()
        def _h(c=conn):
            try:
                record = json.loads(_recv_all(c).decode())
                apply_record(record)
                c.sendall(b"ACK")
                L("OK", f"Replicacao recebida | cliente={record.get('client_id')}")
            except (OSError, json.JSONDecodeError):
                pass
            finally:
                c.close()
        threading.Thread(target=_h, daemon=True).start()

def thread_ping_server() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("", 7072)); srv.listen(10)
    while True:
        conn, _ = srv.accept()
        try:
            conn.recv(16); conn.sendall(b"PONG")
        except OSError:
            pass
        finally:
            conn.close()

def thread_transfer_server() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("", 7073)); srv.listen(10)
    L("INFO", "TRANSFER server porta 7073 pronto")
    while True:
        conn, _ = srv.accept()
        def _h(c=conn):
            try:
                msg   = json.loads(_recv_all(c).decode())
                mtype = msg.get("type")
                if mtype == "REQUEST_TRANSFER":
                    novo = msg["novo_primario"]
                    snap = get_data_snapshot()
                    c.sendall(json.dumps({"status": "TRANSFERRED", "snapshot": snap}).encode())
                    set_primary(novo)
                    L("TRANSFER", f"Dados enviados para store{novo} -- agora sou backup")
                elif mtype == "NEW_PRIMARY":
                    novo = msg["primary_id"]
                    set_primary(novo)
                    c.sendall(b"OK")
            except (OSError, json.JSONDecodeError) as e:
                L("ERRO", f"Transfer handler: {e}")
            finally:
                c.close()
        threading.Thread(target=_h, daemon=True).start()

def thread_heartbeat() -> None:
    time.sleep(8)
    falhas: dict = {}
    while True:
        time.sleep(PING_INTERVAL)
        for i, host in enumerate(STORE_HOSTS):
            if i == MEU_ID:
                continue
            alive = ping_store(host)
            with _met_lock:
                _met["pings_enviados"] += 1
            if not alive:
                with _met_lock:
                    _met["pings_falhos"] += 1
                falhas[i] = falhas.get(i, 0) + 1
                if falhas[i] >= 2 and get_primary() == i:
                    candidato = MEU_ID
                    for j, h in enumerate(STORE_HOSTS):
                        if j == i:
                            continue
                        if j < candidato and ping_store(h):
                            candidato = j
                    if candidato == MEU_ID:
                        separador(tipo="store")
                        L("ELEICAO", f"store{i} inativo -- assumindo primario por eleicao")
                        separador(tipo="store")
                        set_primary(MEU_ID)
                        with _met_lock:
                            _met["failovers"] += 1
                        notificar_novo_primario()
            else:
                falhas.pop(i, None)

def thread_metrics() -> None:
    while True:
        time.sleep(15)
        with _met_lock:
            m = _met.copy()
        pid  = get_primary()
        role = "PRIMARIO" if pid == MEU_ID else f"BACKUP (primario=store{pid})"
        with _data_lock:
            versao = _store_data.get("version", 0)

        separador(f"METRICAS -- Store {MEU_ID} | {role} | versao={versao}", tipo="store")
        rows = [
            ("Writes recebidos",        m["writes_recebidos"]),
            ("Writes executados",        m["writes_executados"]),
            ("Replicacoes async OK",     m["replicacoes_async_ok"]),
            ("Replicacoes async FAIL",   m["replicacoes_async_fail"]),
            ("Transfers como origem",    m["transfers_como_origem"]),
            ("Transfers como destino",   m["transfers_como_destino"]),
            ("Failovers",                m["failovers"]),
            ("Pings enviados",           m["pings_enviados"]),
            ("Pings falhos",             m["pings_falhos"]),
        ]
        for label, valor in rows:
            log(f"STORE {MEU_ID}|{papel()}", "METRICA", f"{label:<30} {valor}", tipo="store")


if __name__ == "__main__":
    MEU_ID = int(get_env_var("MY_STORE_ID", "0"))
    separador(f"INICIANDO STORE {MEU_ID}  |  Protocolo 2  |  primario inicial=store{get_primary()}", tipo="store")
    threads = [
        threading.Thread(target=thread_write_server,       daemon=True, name="write-srv"),
        threading.Thread(target=thread_replication_server, daemon=True, name="repl-srv"),
        threading.Thread(target=thread_ping_server,        daemon=True, name="ping-srv"),
        threading.Thread(target=thread_transfer_server,    daemon=True, name="transfer-srv"),
        threading.Thread(target=thread_heartbeat,          daemon=True, name="heartbeat"),
        threading.Thread(target=thread_metrics,            daemon=True, name="metrics"),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
