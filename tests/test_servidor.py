"""Testes do servidor da caixa de perguntas. Não chamam Gemini nem Databricks:
o agente é trocado por uma função falsa (monkeypatch)."""
import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

import servidor

CHAVES_FALSAS = {
    "GEMINI_API_KEY": "AIzaCHAVEFALSA1234567890",
    "DATABRICKS_SERVER_HOSTNAME": "dbc-teste.cloud.databricks.com",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/abc123",
    "DATABRICKS_TOKEN": "dapiTOKENFALSO1234567890",
}


@pytest.fixture
def base(monkeypatch):
    for k, v in CHAVES_FALSAS.items():
        monkeypatch.setenv(k, v)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), servidor.Tratador)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def chamar(url, corpo=None):
    dados = None if corpo is None else json.dumps(corpo).encode()
    req = urllib.request.Request(url, data=dados, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_validar_pergunta():
    assert servidor.validar_pergunta("  Qual   a taxa? ") == (True, "Qual a taxa?")
    assert servidor.validar_pergunta("")[0] is False
    assert servidor.validar_pergunta(None)[0] is False
    assert servidor.validar_pergunta("x" * 501)[0] is False


def test_saude_nao_expoe_chaves(base):
    status, j = chamar(base + "/api/saude")
    assert status == 200 and j == {"ok": True, "configurado": True}
    texto = json.dumps(j)
    for v in CHAVES_FALSAS.values():
        assert v not in texto


def test_sem_chaves_responde_503(base, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "AIzaxxxxxxxx")
    status, j = chamar(base + "/api/perguntar", {"pergunta": "Qual a taxa?"})
    assert status == 503 and "agente_ia/.env" in j["erro"]


def test_pergunta_ok(base, monkeypatch):
    monkeypatch.setattr(servidor.agente, "perguntar", lambda p, verbose=False: {
        "pergunta": p, "sql": "SELECT 1\nLIMIT 500", "colunas": ["taxa_pct"],
        "linhas": [(6.46,)], "resposta": "A Iberia cancela 6,46%.",
    })
    status, j = chamar(base + "/api/perguntar", {"pergunta": "Qual a taxa da Iberia?"})
    assert status == 200
    assert j["resposta"] == "A Iberia cancela 6,46%." and j["linhas"] == [[6.46]] and j["total_linhas"] == 1


def test_erro_nao_vaza_token(base, monkeypatch):
    def falha(p, verbose=False):
        raise RuntimeError("falhou com token " + CHAVES_FALSAS["DATABRICKS_TOKEN"])
    monkeypatch.setattr(servidor.agente, "perguntar", falha)
    status, j = chamar(base + "/api/perguntar", {"pergunta": "Qual a taxa?"})
    assert status == 502
    assert CHAVES_FALSAS["DATABRICKS_TOKEN"] not in j["erro"] and "[oculto]" in j["erro"]


def test_pedido_invalido(base):
    assert chamar(base + "/api/perguntar", {"pergunta": ""})[0] == 400
    assert chamar(base + "/api/outra", {"pergunta": "oi"})[0] == 404


def test_gemini_sobrecarregado_vira_mensagem_clara(base, monkeypatch):
    def sobrecarregado(p, verbose=False):
        raise servidor.agente.GeminiIndisponivel("O Gemini está sobrecarregado agora.")
    monkeypatch.setattr(servidor.agente, "perguntar", sobrecarregado)
    status, j = chamar(base + "/api/perguntar", {"pergunta": "Qual a taxa?"})
    assert status == 503 and "sobrecarregado" in j["erro"]


def test_erro_temporario_do_gemini_e_reconhecido():
    class Erro503(Exception):
        code = 503
    assert servidor.agente._erro_temporario(Erro503("x")) is True
    assert servidor.agente._erro_temporario(Exception("503 UNAVAILABLE. This model is currently experiencing high demand")) is True
    assert servidor.agente._erro_temporario(Exception("API key not valid")) is False


def test_resposta_montada_sem_gemini():
    r = servidor.agente.montar_resposta(
        "SELECT ... WHERE entra_em_cancelamento AND cancelamento_operacional", ["taxa_pct"], [(6.46,)])
    assert "6,46" in r
    assert servidor.agente.regras_do_sql("WHERE entra_em_cancelamento AND cancelamento_operacional") == [
        "R1: só os voos elegíveis no denominador", "R2: cancelamento sem os voos-fantasma"]
    assert servidor.agente.montar_resposta("SELECT 1", ["x"], []) == "A consulta não retornou nenhuma linha."
    r = servidor.agente.montar_resposta("SELECT 1", ["hora", "partidas"], [(11, 1234), (12, 1000)])
    assert "2 linhas" in r and "1.234" in r


def test_frase_preenchida_com_o_dado():
    ag = servidor.agente
    assert ag.preencher_frase("A taxa da Iberia é de {taxa_cancelamento}%.", ["taxa_cancelamento"], [(6.53,)]) \
        == "A taxa da Iberia é de 6,53%."
    assert ag.preencher_frase("O pico é às {hora}h, com {partidas} partidas.", ["hora", "partidas"], [(11, 1555), (3, 1501)]) \
        == "O pico é às 11h, com 1.555 partidas."
    # número escrito pelo próprio Gemini: recusa (volta para a resposta em lista)
    assert ag.preencher_frase("A taxa é {taxa}%, contra 2,29% no Brasil.", ["taxa"], [(6.53,)]) is None
    # marcador que não existe, ou resultado vazio: recusa
    assert ag.preencher_frase("A taxa é {outra}%.", ["taxa"], [(6.53,)]) is None
    assert ag.preencher_frase("A taxa é {taxa}%.", ["taxa"], []) is None
    # nome de regra não conta como número inventado
    assert ag.preencher_frase("Pela R2, a taxa é {taxa}%.", ["taxa"], [(6.53,)]) == "Pela R2, a taxa é 6,53%."
