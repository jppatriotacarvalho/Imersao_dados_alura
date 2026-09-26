"""
VoeBem — Script de Teste de Aceitação da Governança e Regras de Negócio (Fase 4 & 5)
Avalia se o Agente de IA respeita o contrato de dados e as 6 regras autorais.
Executa contra o Databricks SQL Warehouse e gera o relatório oficial.
"""

import os
import re
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Carrega ambiente
env_path = Path(__file__).parent / ".env"
if not env_path.exists():
    env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

try:
    from agente import gerar_sql_com_gemini, validar_sql, executar_no_databricks, MODELO_PADRAO
except ImportError:
    from agente_ia.agente import gerar_sql_com_gemini, validar_sql, executar_no_databricks, MODELO_PADRAO

CASOS_DE_TESTE = [
    {
        "id": 1,
        "regra": "R2 (Cancelamento Real)",
        "pergunta": "Qual a taxa de cancelamento da Iberia?",
        "esperado": 6.53,  # R1 + R2: entra_em_cancelamento e sem voos-fantasma no denominador
        "tolerancia": 0.5,
        "coluna_alvo": r"cancel|taxa|pct",
        "sql_deve_conter": [r"cancelamento_operacional", r"IBE"],
        "sql_nao_pode_conter": [r"\bvoo_cancelado\b"],
    },
    {
        "id": 2,
        "regra": "Cancelamento Bruto Explícito",
        "pergunta": "Qual a taxa de cancelamento bruta da Iberia, sem filtrar nada?",
        "esperado": 54.61,
        "tolerancia": 1.0,
        "coluna_alvo": r"cancel|taxa|pct",
        "sql_deve_conter": [r"\bvoo_cancelado\b", r"IBE"],
        "sql_nao_pode_conter": [],
    },
    {
        "id": 3,
        "regra": "R3 (Hora Local de Origem)",
        "pergunta": "Qual o horário de pico de partidas em Manaus?",
        "esperado": 11,
        "tolerancia": 0,
        "coluna_alvo": r"hora",
        "sql_deve_conter": [r"hora_local_origem"],
        "sql_nao_pode_conter": [r"\bhora_brasilia\b"],
    },
    {
        "id": 4,
        "regra": "R1 + R2 (Cancelamento Nacional)",
        "pergunta": "Qual a taxa de cancelamento geral do Brasil no período?",
        "esperado": 2.29,  # R1 + R2: entra_em_cancelamento e sem voos-fantasma no denominador
        "tolerancia": 0.1,
        "coluna_alvo": r"cancel|taxa|pct",
        "sql_deve_conter": [r"entra_em_cancelamento", r"cancelamento_operacional"],
        "sql_nao_pode_conter": [],
    },
    {
        "id": 5,
        "regra": "R1 (Denominador DI ≠ 0, 2)",
        "pergunta": "Um voo com código DI igual a 3 pode ser cancelado?",
        "esperado": 0.0,
        "tolerancia": 0.05,
        "coluna_alvo": r"cancel|taxa|pct",
        "sql_deve_conter": [r"codigo_di\s*(=|in)\s*\(?\s*'?3'?"],
        "sql_nao_pode_conter": [],
    },
    {
        "id": 6,
        "regra": "R6 (Percentil 90 vs Média)",
        "pergunta": "Compare o atraso médio e o p90 da rota SBNF-SBGR da LATAM e SBGR-SBGO da GOL",
        "esperado": 40.0,
        "tolerancia": 3.0,
        "coluna_alvo": r"p90|percentil",
        "linha_contendo": "SBNF",
        "sql_deve_conter": [r"percentile", r"SBNF", r"SBGR"],
        "sql_nao_pode_conter": [],
    },
    {
        "id": 7,
        "regra": "R5 (Dia Atípico)",
        "pergunta": "Qual dia teve o maior índice de cancelamento no período?",
        "esperado": 17.85,
        "tolerancia": 0.5,
        "coluna_alvo": r"cancel|taxa|pct",
        "resultado_deve_conter": r"2025-12-10|10/12/2025",
        "sql_deve_conter": [r"cancel", r"order\s+by", r"desc"],
        "sql_nao_pode_conter": [],
    },
    {
        "id": 8,
        "regra": "Linhagem vs Qualidade (Turkish)",
        "pergunta": "A Turkish Airlines tem uma taxa alta de cancelamento?",
        "esperado": 1.46,  # R1 + R2 (o ranking do painel, sem R1, dá 1,44%)
        "tolerancia": 0.6,
        "coluna_alvo": r"cancel|taxa|pct",
        "sql_deve_conter": [r"THY"],
        "sql_nao_pode_conter": [r"registro_malha_internacional"],
    },
]


def avaliar_caso(
    caso: dict,
    sql: str,
    colunas: list[str] | None,
    linhas: list | None,
    somente_sql: bool = False,
) -> dict:
    """
    Retorna {"status": "PASSA" | "FALHA" | "NAO_AVALIADO", "detalhe": str, "obtido": ...}.
    Função pura: não chama API nem banco de dados. Testável com pytest.
    """
    # 1. Validação de formato e segurança do SQL
    valido, msg_sql = validar_sql(sql)
    if not valido:
        return {"status": "FALHA", "detalhe": f"SQL inválido: {msg_sql}", "obtido": None}

    # 2. Padrões de conformidade da governança (deve conter / não pode conter)
    for pat in caso.get("sql_deve_conter", []):
        if not re.search(pat, sql, re.IGNORECASE):
            return {
                "status": "FALHA",
                "detalhe": f"SQL não contém padrão obrigatório: '{pat}'",
                "obtido": None,
            }

    for pat in caso.get("sql_nao_pode_conter", []):
        if re.search(pat, sql, re.IGNORECASE):
            return {
                "status": "FALHA",
                "detalhe": f"SQL contém padrão proibido: '{pat}'",
                "obtido": None,
            }

    # 3. Se flag somente_sql estiver ligada, a governança passou mas o número não foi checado
    if somente_sql:
        return {
            "status": "NAO_AVALIADO",
            "detalhe": "Governança SQL OK (execução omitida com --sql-only)",
            "obtido": None,
        }

    # 4. Resultado vazio
    if not linhas or len(linhas) == 0 or not colunas:
        return {"status": "FALHA", "detalhe": "Resultado da consulta vazio", "obtido": None}

    # 5. Localizar coluna do número pela regex coluna_alvo
    coluna_alvo_regex = caso.get("coluna_alvo")
    idx_col = None
    if coluna_alvo_regex:
        for idx, col in enumerate(colunas):
            if re.search(coluna_alvo_regex, str(col), re.IGNORECASE):
                idx_col = idx
                break
        if idx_col is None:
            return {
                "status": "FALHA",
                "detalhe": f"Coluna alvo '{coluna_alvo_regex}' não encontrada nas colunas: {colunas}",
                "obtido": None,
            }
    else:
        idx_col = 0

    # 6. Escolher a linha alvo
    linha_alvo = None
    filtro_linha = caso.get("linha_contendo")
    if filtro_linha:
        for l in linhas:
            if any(filtro_linha.lower() in str(cel).lower() for cel in l):
                linha_alvo = l
                break
        if linha_alvo is None:
            return {
                "status": "FALHA",
                "detalhe": f"Nenhuma linha contém o filtro '{filtro_linha}'",
                "obtido": None,
            }
    else:
        linha_alvo = linhas[0]

    # 7. Converter para float
    valor_celula = linha_alvo[idx_col]
    if valor_celula is None:
        return {
            "status": "FALHA",
            "detalhe": f"Valor nulo na coluna alvo ({colunas[idx_col]})",
            "obtido": None,
        }

    try:
        obtido = float(str(valor_celula).strip().replace("%", ""))
    except (ValueError, TypeError):
        return {
            "status": "FALHA",
            "detalhe": f"Não foi possível converter '{valor_celula}' para float",
            "obtido": None,
        }

    # 8. Comparar com tolerância estrita
    esperado = caso["esperado"]
    tol = caso.get("tolerancia", 0.0)
    if abs(obtido - esperado) > (tol + 1e-9):
        return {
            "status": "FALHA",
            "detalhe": f"Valor {obtido} fora da tolerância (esperado: {esperado} ± {tol})",
            "obtido": obtido,
        }

    # 9. Checar regex no resultado se houver
    res_regex = caso.get("resultado_deve_conter")
    if res_regex:
        casou = any(re.search(res_regex, str(cel), re.IGNORECASE) for cel in linha_alvo)
        if not casou:
            return {
                "status": "FALHA",
                "detalhe": f"Linha não contém padrão esperado '{res_regex}': {linha_alvo}",
                "obtido": obtido,
            }

    # 10. Aprovado
    return {
        "status": "PASSA",
        "detalhe": f"Obtido: {obtido} (esperado: {esperado} ± {tol})",
        "obtido": obtido,
    }


def _celula_md(texto) -> str:
    """Deixa o texto seguro para uma célula de tabela Markdown ('|' e quebra de linha quebram a tabela)."""
    return str(texto).replace("|", "\\|").replace("\n", " ")


def rodar_teste_aceitacao(dry_run_sql_only: bool = False):
    """Executa o placar de testes contra o modelo e o Databricks."""
    modelo_utilizado = os.getenv("GEMINI_MODEL", MODELO_PADRAO)
    print("\n" + "=" * 80)
    print(f"🏆  VoeBem — Placar de Teste de Aceitação da Governança ({modelo_utilizado})")
    print("=" * 80)

    resultados_placar = []
    relatorio_md_linhas = [
        "# Relatório de Aceitação — Agente de IA VoeBem",
        f"- **Data/Hora:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- **Modelo:** `{modelo_utilizado}`",
        f"- **Modo:** `{'Somente SQL (--sql-only)' if dry_run_sql_only else 'Completo (SQL + Databricks)'}`",
        "",
        "| # | Regra | Pergunta | Status | Esperado | Obtido | Detalhes |",
        "|---|---|---|---|---|---|---|",
    ]
    detalhes_sql = []

    for caso in CASOS_DE_TESTE:
        cid = caso["id"]
        regra = caso["regra"]
        pergunta = caso["pergunta"]
        esperado = caso["esperado"]

        print(f"\n[{cid}/8] Avaliando: \"{pergunta}\" ({regra})")

        sql_gerado = ""
        try:
            sql_gerado = gerar_sql_com_gemini(pergunta)
            valido, sql_limpo = validar_sql(sql_gerado)

            if not valido:
                # sql_limpo aqui é a mensagem de erro; o relatório guarda o SQL original
                resultado = {"status": "FALHA", "detalhe": f"SQL Inválido: {sql_limpo}", "obtido": None}
            elif dry_run_sql_only:
                resultado = avaliar_caso(caso, sql_limpo, None, None, somente_sql=True)
                sql_gerado = sql_limpo
            else:
                colunas, linhas = executar_no_databricks(sql_limpo)
                resultado = avaliar_caso(caso, sql_limpo, colunas, linhas, somente_sql=False)
                sql_gerado = sql_limpo

        except Exception as e:
            resultado = {"status": "ERRO", "detalhe": str(e), "obtido": None}

        status = resultado["status"]
        detalhe = resultado["detalhe"]
        obtido = resultado.get("obtido")
        if obtido is None:
            obtido = "—"

        print(f"Status: {status} | Detalhes: {detalhe}")
        resultados_placar.append((cid, regra, status, esperado, obtido, detalhe, sql_gerado))
        relatorio_md_linhas.append(
            f"| {cid} | {regra} | {pergunta} | **{status}** | {esperado} | {obtido} | {_celula_md(detalhe)} |"
        )
        detalhes_sql.append(f"### Caso {cid}: {pergunta}\n**SQL Gerado:**\n```sql\n{sql_gerado}\n```\n")

    # Contagem de resultados
    passa = sum(1 for r in resultados_placar if r[2] == "PASSA")
    falha = sum(1 for r in resultados_placar if r[2] == "FALHA")
    erro = sum(1 for r in resultados_placar if r[2] == "ERRO")
    nao_avaliado = sum(1 for r in resultados_placar if r[2] == "NAO_AVALIADO")

    print("\n" + "=" * 80)
    print(f"📊 PLACAR FINAL: {passa} PASSA · {falha} FALHA · {erro} ERRO · {nao_avaliado} NÃO AVALIADO")
    print("=" * 80)
    print(f"{'#':<3} | {'Regra':<28} | {'Status':<12} | {'Esperado':<9} | {'Obtido':<8} | {'Detalhes'}")
    print("-" * 80)
    for r in resultados_placar:
        print(f"{r[0]:<3} | {r[1]:<28} | {r[2]:<12} | {str(r[3]):<9} | {str(r[4]):<8} | {r[5]}")
    print("=" * 80 + "\n")

    # Grava relatório em agente_ia/resultados/ultimo_placar.md
    pasta_resultados = Path(__file__).parent / "resultados"
    pasta_resultados.mkdir(parents=True, exist_ok=True)
    arquivo_md = pasta_resultados / "ultimo_placar.md"

    relatorio_md_linhas.append("")
    relatorio_md_linhas.append(f"**Total:** {passa} PASSA · {falha} FALHA · {erro} ERRO · {nao_avaliado} NÃO AVALIADO")
    relatorio_md_linhas.append("")
    relatorio_md_linhas.append("## Consultas SQL Geradas")
    relatorio_md_linhas.extend(detalhes_sql)

    with open(arquivo_md, "w", encoding="utf-8") as f:
        f.write("\n".join(relatorio_md_linhas))
    print(f"📄 Relatório salvo em: {arquivo_md}")

    # Código de saída diferente de 0 se houver FALHA ou ERRO
    if falha > 0 or erro > 0:
        sys.exit(1)


if __name__ == "__main__":
    dry_run = "--sql-only" in sys.argv
    rodar_teste_aceitacao(dry_run_sql_only=dry_run)
