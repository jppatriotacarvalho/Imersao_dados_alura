import pytest
from teste_aceitacao import avaliar_caso, CASOS_DE_TESTE


def obter_caso(id_caso: int) -> dict:
    return next(c for c in CASOS_DE_TESTE if c["id"] == id_caso)


def test_caso1_sucesso():
    caso = obter_caso(1)
    sql = "SELECT ROUND(100.0 * AVG(CASE WHEN cancelamento_operacional THEN 1 ELSE 0 END), 2) AS taxa_cancelamento FROM voebem.gold.obt_voos WHERE sigla_icao_empresa = 'IBE' LIMIT 500"
    colunas = ["taxa_cancelamento"]
    linhas = [(6.46,)]
    res = avaliar_caso(caso, sql, colunas, linhas)
    assert res["status"] == "PASSA"


def test_caso1_falha_usou_voo_cancelado():
    caso = obter_caso(1)
    sql = "SELECT ROUND(100.0 * AVG(CASE WHEN voo_cancelado THEN 1 ELSE 0 END), 2) AS taxa_cancelamento FROM voebem.gold.obt_voos WHERE sigla_icao_empresa = 'IBE' AND cancelamento_operacional LIMIT 500"
    colunas = ["taxa_cancelamento"]
    linhas = [(6.46,)]
    res = avaliar_caso(caso, sql, colunas, linhas)
    assert res["status"] == "FALHA"
    assert "padrão proibido" in res["detalhe"]


def test_caso1_falha_taxa_bruta_54_61():
    caso = obter_caso(1)
    sql = "SELECT ROUND(100.0 * AVG(CASE WHEN cancelamento_operacional THEN 1 ELSE 0 END), 2) AS taxa_cancelamento FROM voebem.gold.obt_voos WHERE sigla_icao_empresa = 'IBE' LIMIT 500"
    colunas = ["taxa_cancelamento"]
    linhas = [(54.61,)]
    res = avaliar_caso(caso, sql, colunas, linhas)
    assert res["status"] == "FALHA"
    assert "fora da tolerância" in res["detalhe"]


def test_caso3_falha_hora_brasilia_12():
    caso = obter_caso(3)
    # Prova o conserto da tolerância zero: se der 12 (hora de Brasília), deve falhar
    sql = "SELECT hora_local_origem, COUNT(*) FROM voebem.gold.obt_voos WHERE sigla_icao_aerodromo_origem = 'SBEG' GROUP BY hora_local_origem ORDER BY 2 DESC LIMIT 500"
    colunas = ["hora_local_origem", "total"]
    linhas = [(12, 500)]
    res = avaliar_caso(caso, sql, colunas, linhas)
    assert res["status"] == "FALHA"
    assert "fora da tolerância" in res["detalhe"]


def test_caso4_falha_taxa_bruta_2_87():
    caso = obter_caso(4)
    # Prova o conserto da tolerância 0.1: se der 2.87 (taxa bruta), deve falhar
    sql = "SELECT ROUND(100.0 * AVG(CASE WHEN cancelamento_operacional THEN 1 ELSE 0 END), 2) AS taxa FROM voebem.gold.obt_voos WHERE entra_em_cancelamento = true LIMIT 500"
    colunas = ["taxa"]
    linhas = [(2.87,)]
    res = avaliar_caso(caso, sql, colunas, linhas)
    assert res["status"] == "FALHA"
    assert "fora da tolerância" in res["detalhe"]


def test_caso6_falha_seleciona_coluna_p90_correta():
    caso = obter_caso(6)
    # Se a query retornar rota, media e p90, avaliar_caso deve pegar a coluna do p90, não a média!
    sql = "SELECT 'SBNF' as rota, 40.0 as atraso_medio, 7.5 as p90_atraso, percentile_approx(atraso, 0.9) from voebem.gold.obt_voos where sigla_icao_aerodromo_origem = 'SBNF' and sigla_icao_aerodromo_destino = 'SBGR' LIMIT 500"
    colunas = ["rota", "atraso_medio", "p90_atraso"]
    linhas = [("SBNF", 40.0, 7.5)]  # Aqui p90 está 7.5, logo deve FALHAR pois esperado é 40.0
    res = avaliar_caso(caso, sql, colunas, linhas)
    assert res["status"] == "FALHA"
    assert "fora da tolerância" in res["detalhe"]


def test_caso7_falha_data_incorreta():
    caso = obter_caso(7)
    sql = "SELECT data_partida_prevista, ROUND(100.0 * AVG(CASE WHEN cancelamento_operacional THEN 1 ELSE 0 END), 2) as taxa_cancelamento FROM voebem.gold.obt_voos GROUP BY 1 ORDER BY taxa_cancelamento DESC LIMIT 500"
    colunas = ["data_partida_prevista", "taxa_cancelamento"]
    linhas = [("2025-12-11", 17.85)]  # Data errada (11 ao invés de 10)
    res = avaliar_caso(caso, sql, colunas, linhas)
    assert res["status"] == "FALHA"
    assert "padrão esperado" in res["detalhe"]


def test_qualquer_caso_resultado_vazio():
    caso = obter_caso(1)
    sql = "SELECT cancelamento_operacional FROM voebem.gold.obt_voos WHERE sigla_icao_empresa = 'IBE' LIMIT 500"
    res = avaliar_caso(caso, sql, ["cancelamento_operacional"], [])
    assert res["status"] == "FALHA"
    assert "vazio" in res["detalhe"]


def test_qualquer_caso_coluna_alvo_nao_encontrada():
    caso = obter_caso(1)
    sql = "SELECT sigla_icao_empresa FROM voebem.gold.obt_voos WHERE sigla_icao_empresa = 'IBE' AND cancelamento_operacional LIMIT 500"
    res = avaliar_caso(caso, sql, ["sigla_icao_empresa"], [("IBE",)])
    assert res["status"] == "FALHA"
    assert "não encontrada" in res["detalhe"]


def test_somente_sql_governanca_ok_retorna_nao_avaliado():
    caso = obter_caso(1)
    sql = "SELECT cancelamento_operacional FROM voebem.gold.obt_voos WHERE sigla_icao_empresa = 'IBE' LIMIT 500"
    res = avaliar_caso(caso, sql, None, None, somente_sql=True)
    assert res["status"] == "NAO_AVALIADO"


def test_somente_sql_governanca_quebrada_retorna_falha():
    caso = obter_caso(1)
    sql = "SELECT voo_cancelado FROM voebem.gold.obt_voos WHERE sigla_icao_empresa = 'IBE' LIMIT 500"
    res = avaliar_caso(caso, sql, None, None, somente_sql=True)
    assert res["status"] == "FALHA"


# ---------- controles positivos ----------
# Os testes acima esperam FALHA. Estes dois provam que a FALHA deles vem da
# regra certa, e não de um avaliar_caso que reprova tudo.

def test_caso6_passa_quando_p90_certo():
    caso = obter_caso(6)
    sql = "SELECT rota, ROUND(AVG(atraso),2) AS atraso_medio, percentile_approx(atraso, 0.9) AS p90_atraso FROM voebem.gold.obt_voos WHERE rota IN ('SBNF-SBGR','SBGR-SBGO') GROUP BY rota LIMIT 500"
    colunas = ["rota", "atraso_medio", "p90_atraso"]
    linhas = [("SBGR-SBGO", 9.1, 25.0), ("SBNF-SBGR", 7.5, 40.0)]  # linha SBNF não é a primeira
    res = avaliar_caso(caso, sql, colunas, linhas)
    assert res["status"] == "PASSA", res["detalhe"]
    assert res["obtido"] == 40.0


def test_caso7_passa_quando_data_certa():
    caso = obter_caso(7)
    sql = "SELECT data_partida_prevista, ROUND(100.0 * AVG(CASE WHEN cancelamento_operacional THEN 1 ELSE 0 END), 2) as taxa_cancelamento FROM voebem.gold.obt_voos GROUP BY 1 ORDER BY taxa_cancelamento DESC LIMIT 500"
    colunas = ["data_partida_prevista", "taxa_cancelamento"]
    linhas = [("2025-12-10", 17.85)]
    res = avaliar_caso(caso, sql, colunas, linhas)
    assert res["status"] == "PASSA", res["detalhe"]
