"""
utils_log.py -- Módulo centralizado de logging com timestamps

Formato de cada linha:
  [HH:MM:SS.mmm]  CONTAINER       NIVEL       mensagem

Arquivos gerados em /app/logs/:
  nodes.log   -- TOKEN, CS, COMMITTED, INFO, OK  (Cluster Sync + Clients)
  store.log   -- WRITE, TRANSFER, OK, ELEICAO    (Cluster Store)
  errors.log  -- FALHA, ERRO, ELEICAO            (qualquer container)
  metrics.log -- APENAS métricas                 (painel dedicado na TUI)

Todos os arquivos são limpos a cada nova execução (modo "w").
"""

import os
import sys
import threading
from datetime import datetime

_lock = threading.Lock()

os.makedirs("/app/logs", exist_ok=True)

_f_nodes   = open("/app/logs/nodes.log",   "w", buffering=1, encoding="utf-8")
_f_store   = open("/app/logs/store.log",   "w", buffering=1, encoding="utf-8")
_f_errors  = open("/app/logs/errors.log",  "w", buffering=1, encoding="utf-8")
_f_metrics = open("/app/logs/metrics.log", "w", buffering=1, encoding="utf-8")

_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
_cab = f"# === EXECUCAO INICIADA EM {_inicio} ===\n\n"
for _f in (_f_nodes, _f_store, _f_errors, _f_metrics):
    _f.write(_cab)

# ── Classificação dos níveis ─────────────────────────────────────────────────
_NIVEIS_ERRO    = {"FALHA", "ERRO"}
_NIVEIS_METRICA = {"METRICA"}
_NIVEIS_ALERTA  = {"ELEICAO"}   # vai para errors.log E nodes/store

_GRUPOS = {
    "TOKEN":     "token",
    "INFO":      "info",
    "OK":        "ok",
    "COMMITTED": "ok",
    "CS":        "critica",
    "WRITE":     "escrita",
    "TRANSFER":  "escrita",
    "ESPERA":    "espera",
    "WARN":      "aviso",
    "FALHA":     "falha",
    "ERRO":      "falha",
    "ELEICAO":   "eleicao",
    "METRICA":   "metrica",
}

_ultimo = {"node": "", "store": ""}


def _ts() -> str:
    """Timestamp colorido: [HH:MM:SS.mmm]"""
    now = datetime.now()
    return f"[{now.strftime('%H:%M:%S')}.{now.microsecond // 1000:03d}]"


def _espaco(tipo: str, nivel: str) -> bool:
    ult = _ultimo.get(tipo, "")
    return ult != "" and _GRUPOS.get(nivel, "x") != _GRUPOS.get(ult, "x")


def log(container: str, nivel: str, msg: str, tipo: str = "node") -> None:
    """
    container : "NO 0", "STORE 1|PRI", "CLI 102", etc.
    nivel     : TOKEN | INFO | OK | COMMITTED | CS | WRITE | TRANSFER |
                ELEICAO | FALHA | ERRO | ESPERA | WARN | METRICA
    msg       : texto livre
    tipo      : "node" | "store"
    """
    ts    = _ts()
    linha = f"{ts}  {container:<16} {nivel:<10} {msg}\n"

    with _lock:
        pref = "\n" if _espaco(tipo, nivel) else ""

        # METRICA → somente metrics.log + stdout
        if nivel in _NIVEIS_METRICA:
            sys.stdout.write(pref + linha)
            sys.stdout.flush()
            _f_metrics.write(pref + linha)
            _ultimo[tipo] = nivel
            return

        # Todos os outros → stdout
        sys.stdout.write(pref + linha)
        sys.stdout.flush()

        # Erros críticos → somente errors.log
        if nivel in _NIVEIS_ERRO:
            _f_errors.write(pref + linha)
            _ultimo[tipo] = nivel
            return

        # Alertas → errors.log + nodes/store
        if nivel in _NIVEIS_ALERTA:
            _f_errors.write(pref + linha)
            if tipo == "store":
                _f_store.write(pref + linha)
            else:
                _f_nodes.write(pref + linha)
            _ultimo[tipo] = nivel
            return

        # Eventos normais
        if tipo == "store":
            _f_store.write(pref + linha)
        else:
            _f_nodes.write(pref + linha)

        _ultimo[tipo] = nivel


def separador(titulo: str = "", tipo: str = "node") -> None:
    """
    Bloco visual delimitado com título.
      separador("")        →  linha simples ──── (para erros/falhas)
      separador("titulo")  →  bloco ┌──┐ │ titulo │ └──┘
    """
    ts    = _ts()
    borda = "─" * 62

    if titulo:
        bloco = (
            f"\n{ts}  ┌{borda}┐\n"
            f"{ts}  │  {titulo:<60}  │\n"
            f"{ts}  └{borda}┘\n"
        )
    else:
        bloco = f"\n{ts}  {borda}\n\n"

    with _lock:
        sys.stdout.write(bloco)
        sys.stdout.flush()

        if titulo:
            if tipo == "store":
                _f_store.write(bloco)
            else:
                _f_nodes.write(bloco)
        else:
            _f_errors.write(bloco)

        _ultimo[tipo] = ""
