"""
VoeBem — servidor local do agente (a caixa de perguntas do site).

O navegador nunca vê as chaves. O fluxo é:
  navegador --POST /api/perguntar--> nginx (web) --rede interna do Docker--> este servidor
  este servidor lê as chaves do agente_ia/.env, chama o agente e devolve só o resultado.

A porta 8000 não é publicada para fora do Docker: só o nginx fala com ela.
Usa apenas a biblioteca padrão do Python (sem dependência nova).
"""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import agente

PORTA = int(os.getenv("PORTA_API", "8000"))
TAMANHO_MAX_PERGUNTA = 500      # caracteres
TAMANHO_MAX_CORPO = 4096        # bytes do JSON recebido
LINHAS_MAX_RESPOSTA = 50        # linhas do resultado devolvidas ao navegador

CHAVES = ["GEMINI_API_KEY", "DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"]

# Uma pergunta por vez: evita gasto duplicado de API se alguém clicar várias vezes.
_ocupado = threading.Lock()


def configurado() -> bool:
    """True quando as quatro variáveis existem e não são o texto de exemplo (xxxx)."""
    for nome in CHAVES:
        valor = os.getenv(nome, "")
        if not valor or "xxxx" in valor:
            return False
    return True


def validar_pergunta(texto) -> tuple[bool, str]:
    """Confere o que veio do navegador antes de gastar qualquer chamada de API."""
    if not isinstance(texto, str):
        return False, "Envie a pergunta como texto."
    texto = " ".join(texto.split())
    if len(texto) < 3:
        return False, "Escreva uma pergunta."
    if len(texto) > TAMANHO_MAX_PERGUNTA:
        return False, f"A pergunta pode ter no máximo {TAMANHO_MAX_PERGUNTA} caracteres."
    return True, texto


def limpar_segredos(mensagem: str) -> str:
    """Tira de uma mensagem de erro qualquer trecho igual a uma chave do .env."""
    for nome in CHAVES:
        valor = os.getenv(nome, "")
        if len(valor) >= 8:
            mensagem = mensagem.replace(valor, "[oculto]")
    return mensagem


def para_json(valor):
    """Datas, Decimal etc. viram texto; números e textos passam como estão."""
    if valor is None or isinstance(valor, (bool, int, float, str)):
        return valor
    return str(valor)


class Tratador(BaseHTTPRequestHandler):
    server_version = "VoeBem"
    sys_version = ""

    def _responder(self, status: int, corpo: dict):
        dados = json.dumps(corpo, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(dados)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(dados)

    def log_message(self, formato, *args):
        # Registra só método, caminho e status; nunca o conteúdo nem cabeçalhos.
        print(f"[api] {self.command} {self.path.split('?')[0]} {args[1] if len(args) > 1 else ''}", flush=True)

    def do_GET(self):
        if self.path.split("?")[0] == "/api/saude":
            self._responder(200, {"ok": True, "configurado": configurado()})
        else:
            self._responder(404, {"erro": "Endereço não encontrado."})

    def do_POST(self):
        if self.path.split("?")[0] != "/api/perguntar":
            self._responder(404, {"erro": "Endereço não encontrado."})
            return

        try:
            tamanho = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            tamanho = 0
        if tamanho <= 0 or tamanho > TAMANHO_MAX_CORPO:
            self._responder(400, {"erro": "Pedido inválido."})
            return
        try:
            corpo = json.loads(self.rfile.read(tamanho).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            self._responder(400, {"erro": "Pedido inválido."})
            return

        ok, pergunta = validar_pergunta(corpo.get("pergunta") if isinstance(corpo, dict) else None)
        if not ok:
            self._responder(400, {"erro": pergunta})
            return

        if not configurado():
            self._responder(503, {"erro": "As chaves ainda não foram preenchidas em agente_ia/.env. "
                                          "Preencha e reinicie com: docker compose up -d --force-recreate api"})
            return

        if not _ocupado.acquire(blocking=False):
            self._responder(429, {"erro": "O agente ainda está respondendo a pergunta anterior. Aguarde."})
            return
        try:
            r = agente.perguntar(pergunta, verbose=False)
            linhas = [[para_json(v) for v in linha] for linha in r["linhas"][:LINHAS_MAX_RESPOSTA]]
            self._responder(200, {
                "pergunta": pergunta,
                "resposta": r["resposta"],
                "regras": r.get("regras", []),
                "sql": r["sql"],
                "colunas": list(r["colunas"]),
                "linhas": linhas,
                "total_linhas": len(r["linhas"]),
            })
        except ValueError as e:        # SQL rejeitado pela validação, ou chave ausente
            self._responder(422, {"erro": limpar_segredos(str(e))})
        except agente.GeminiIndisponivel as e:
            self._responder(503, {"erro": str(e)})
        except Exception as e:         # Gemini ou Databricks fora do ar, warehouse desligado, etc.
            print(f"[api] erro: {type(e).__name__}", flush=True)
            self._responder(502, {"erro": "O agente não conseguiu responder: " + limpar_segredos(str(e))[:300]})
        finally:
            _ocupado.release()


def main():
    servidor = ThreadingHTTPServer(("0.0.0.0", PORTA), Tratador)
    print(f"[api] VoeBem ouvindo na porta {PORTA} (chaves configuradas: {configurado()})", flush=True)
    servidor.serve_forever()


if __name__ == "__main__":
    main()
