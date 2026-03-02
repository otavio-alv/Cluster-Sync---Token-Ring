"""
monitor_tui.py -- TUI de monitoramento do cluster em tempo real

Layout:
  ╔═ Header: Cluster Monitor ·  HH:MM:SS ══════════════════════╗
  ║  Tabs: [ ⬡ SYNC ] [ ⬢ STORE ] [ ✕ ERROS ] [ ◈ MÉTRICAS ] ║
  ║  ┌──────────────────────────────────────────────────────┐   ║
  ║  │  Conteúdo da aba ativa (log ou tabela de métricas)   │   ║
  ║  └──────────────────────────────────────────────────────┘   ║
  ╚═ Footer: q=Sair  c=Limpar ══════════════════════════════════╝

Uso:
  pip install textual
  python monitor_tui.py
"""

import os
import re
from textual.app import App, ComposeResult
from textual.widgets import (
    Header, Footer, RichLog, Static,
    DataTable, TabbedContent, TabPane,
)
from textual.containers import Horizontal

# ── Formato de linha: [HH:MM:SS.mmm]  CONTAINER     NIVEL      msg ──────────
_LINHA_RE = re.compile(
    r"^(\[\d{2}:\d{2}:\d{2}\.\d{3}\])\s+"   # [ts]
    r"((?:NO|STORE|CLI)\s[\w|]+)\s+"              # container
    r"([A-Z_]+)\s+"                                  # nivel
    r"(.+)$"                                          # mensagem
)

# Separadores visuais gerados pelo utils_log.py
_SEP_BORDA_RE = re.compile(r"^\[\d{2}:\d{2}:\d{2}\.\d{3}\]\s+[┌└]")
_SEP_MEIO_RE  = re.compile(r"^\[\d{2}:\d{2}:\d{2}\.\d{3}\]\s+[│]")
_SEP_LINHA_RE = re.compile(r"^\[\d{2}:\d{2}:\d{2}\.\d{3}\]\s+─{20,}")

# ── Cores por nível ──────────────────────────────────────────────────────────
_CORES: dict[str, tuple[str, str]] = {
    "TOKEN":     ("bold yellow",   "white"),
    "INFO":      ("bright_blue",   "white"),
    "OK":        ("bold green",    "white"),
    "COMMITTED": ("bold green",    "bright_green"),
    "CS":        ("bold magenta",  "magenta"),
    "WRITE":     ("cyan",          "white"),
    "TRANSFER":  ("bright_cyan",   "white"),
    "ELEICAO":   ("bold yellow",   "yellow"),
    "FALHA":     ("bold red",      "red"),
    "ERRO":      ("red",           "bright_red"),
    "ESPERA":    ("dim",           "dim"),
    "WARN":      ("yellow",        "white"),
    "METRICA":   ("bright_blue",   "white"),
}

_COR_CONTAINER = {
    "NO":    "bold white",
    "STORE": "bold cyan",
    "CLI":   "bold magenta",
}

_NIVEIS_VALIDOS = set(_CORES.keys())


def _cor_cont(container: str) -> str:
    prefixo = container.split()[0].upper() if container else ""
    return _COR_CONTAINER.get(prefixo, "white")


def _formata_linha(raw: str) -> str:
    """Converte linha de log em markup Rich colorido ou retorna \'\' para ignorar."""
    s = raw.rstrip()
    if not s or s.startswith("#"):
        return ""

    # Separadores de seção crítica / bloco de métricas
    if _SEP_BORDA_RE.match(s):
        return f"[bold cyan]{s}[/]"
    if _SEP_MEIO_RE.match(s):
        return f"[cyan]{s}[/]"
    if _SEP_LINHA_RE.match(s):
        return f"[dim cyan]{s}[/]"

    m = _LINHA_RE.match(s)
    if not m:
        return ""

    ts, container, nivel, msg = m.group(1), m.group(2), m.group(3), m.group(4)
    if nivel not in _NIVEIS_VALIDOS:
        return ""

    cor_c       = _cor_cont(container)
    cor_n, cor_msg = _CORES.get(nivel, ("white", "white"))

    # Realça a mensagem de eventos críticos
    if nivel in ("COMMITTED", "CS", "FALHA", "ELEICAO", "ERRO"):
        cor_msg = cor_n

    return (
        f"[dim white]{ts}[/]  "
        f"[{cor_c}]{container:<16}[/]"
        f"[{cor_n}]{nivel:<11}[/]"
        f"[{cor_msg}]{msg}[/]"
    )


# ── Painel de log (leitura incremental) ─────────────────────────────────────
class LogTab(Static):
    DEFAULT_CSS = """
    LogTab {
        height: 1fr;
        width: 1fr;
    }
    LogTab RichLog {
        height: 1fr;
        border: solid $primary-darken-2;
        padding: 0 1;
        scrollbar-gutter: stable;
    }
    """

    def __init__(self, log_file: str, **kw):
        super().__init__(**kw)
        self.log_file  = log_file
        self.last_size = 0

    def compose(self) -> ComposeResult:
        yield RichLog(highlight=False, markup=True, wrap=False, id=f"rl_{self.id}")

    def on_mount(self) -> None:
        self._rl = self.query_one(RichLog)
        self.set_interval(0.4, self._poll)

    def _poll(self) -> None:
        if not os.path.exists(self.log_file):
            return
        size = os.path.getsize(self.log_file)
        if size == self.last_size:
            return
        with open(self.log_file, "r", encoding="utf-8", errors="replace") as fh:
            fh.seek(0 if self.last_size == 0 else self.last_size)
            for raw in fh:
                fmt = _formata_linha(raw)
                if fmt:
                    self._rl.write(fmt)
        self.last_size = size

    def clear(self) -> None:
        self._rl.clear()
        self.last_size = 0


# ── Painel de métricas: tabela viva por nó ──────────────────────────────────
def _is_positive(val: str) -> bool:
    try:
        return float(val) > 0
    except ValueError:
        return False


class MetricsTab(Static):
    DEFAULT_CSS = """
    MetricsTab {
        height: 1fr;
        layout: vertical;
    }
    MetricsTab Horizontal {
        height: 1fr;
    }
    MetricsTab DataTable {
        height: 1fr;
        width: 2fr;
        border: solid $accent-darken-1;
    }
    MetricsTab RichLog {
        height: 1fr;
        width: 1fr;
        border: solid $primary-darken-2;
        padding: 0 1;
    }
    """

    def __init__(self, **kw):
        super().__init__(**kw)
        self._data: dict[str, dict[str, str]] = {}  # {container: {chave: valor}}
        self.last_size = 0

    def compose(self) -> ComposeResult:
        with Horizontal():
            yield DataTable(id="dt_metrics", zebra_stripes=True, cursor_type="row")
            yield RichLog(
                highlight=False, markup=True, wrap=False, id="rl_metrics_raw"
            )

    def on_mount(self) -> None:
        self._table = self.query_one("#dt_metrics", DataTable)
        self._raw   = self.query_one("#rl_metrics_raw", RichLog)
        self._table.add_columns("  Nó", "  Métrica", "  Valor")
        self.set_interval(1.0, self._poll)

    def _poll(self) -> None:
        log_file = "logs/metrics.log"
        if not os.path.exists(log_file):
            return
        size = os.path.getsize(log_file)
        if size == self.last_size:
            return

        with open(log_file, "r", encoding="utf-8", errors="replace") as fh:
            fh.seek(0 if self.last_size == 0 else self.last_size)
            for raw in fh:
                s = raw.strip()
                if not s or s.startswith("#"):
                    continue

                m = _LINHA_RE.match(s)
                if not m or m.group(3) != "METRICA":
                    continue

                container, msg = m.group(2), m.group(4)
                mv = re.match(r"(.+?)\s{2,}(\S+)$", msg)
                if mv:
                    chave = mv.group(1).strip()
                    valor = mv.group(2)
                    self._data.setdefault(container, {})[chave] = valor

                    # Exibe no painel raw também
                    cor_c = _cor_cont(container)
                    cor_v = "bold green" if _is_positive(valor) else "dim white"
                    ts    = m.group(1)
                    self._raw.write(
                        f"[dim]{ts}[/]  [{cor_c}]{container:<16}[/]"
                        f"[white]{chave:<34}[/][{cor_v}]{valor:>8}[/]"
                    )

        self.last_size = size
        self._rebuild_table()

    def _rebuild_table(self) -> None:
        self._table.clear()
        prev_container = None
        for container in sorted(self._data):
            cor_c = _cor_cont(container)
            # Linha de separação entre nós diferentes
            if prev_container is not None and prev_container != container:
                self._table.add_row("", "─" * 36, "")
            for chave, valor in self._data[container].items():
                cor_v = "bold green" if _is_positive(valor) else "dim white"
                self._table.add_row(
                    f"[{cor_c}] {container}[/]",
                    f"  {chave}",
                    f"[{cor_v}]  {valor}[/]",
                )
            prev_container = container

    def clear(self) -> None:
        self._data.clear()
        self._table.clear()
        self._raw.clear()
        self.last_size = 0


# ── App principal ────────────────────────────────────────────────────────────
class MonitorApp(App):
    CSS = """
    Screen {
        layout: vertical;
        background: $surface;
    }
    TabbedContent {
        height: 1fr;
    }
    TabbedContent ContentSwitcher {
        height: 1fr;
    }
    TabPane {
        padding: 0 1;
        height: 1fr;
    }
    """

    TITLE   = "Cluster Monitor  ·  Token Ring + Store Sync"
    BINDINGS = [
        ("q",   "quit",    "Sair"),
        ("c",   "limpar",  "Limpar"),
        ("1",   "tab_sync",     "SYNC"),
        ("2",   "tab_store",    "STORE"),
        ("3",   "tab_errors",   "ERROS"),
        ("4",   "tab_metrics",  "MÉTRICAS"),
    ]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with TabbedContent(initial="tab_sync"):
            with TabPane("⬡  SYNC",     id="tab_sync"):
                yield LogTab("logs/nodes.log", id="p_sync")
            with TabPane("⬢  STORE",    id="tab_store"):
                yield LogTab("logs/store.log", id="p_store")
            with TabPane("✕  ERROS",    id="tab_errors"):
                yield LogTab("logs/errors.log", id="p_errors")
            with TabPane("◈  MÉTRICAS", id="tab_metrics"):
                yield MetricsTab(id="p_metrics")
        yield Footer()

    def action_limpar(self) -> None:
        for wid in ("p_sync", "p_store", "p_errors"):
            try:
                self.query_one(f"#{wid}", LogTab).clear()
            except Exception:
                pass
        try:
            self.query_one("#p_metrics", MetricsTab).clear()
        except Exception:
            pass

    def action_tab_sync(self)    -> None: self.query_one(TabbedContent).active = "tab_sync"
    def action_tab_store(self)   -> None: self.query_one(TabbedContent).active = "tab_store"
    def action_tab_errors(self)  -> None: self.query_one(TabbedContent).active = "tab_errors"
    def action_tab_metrics(self) -> None: self.query_one(TabbedContent).active = "tab_metrics"


if __name__ == "__main__":
    if not os.path.exists("logs"):
        print("ERRO: Pasta logs/ não encontrada!")
        print("Execute no diretório do projeto (onde docker-compose.yml está).")
        exit(1)
    MonitorApp().run()
