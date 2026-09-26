"""
VoeBem — Agente de Inteligência Texto -> SQL sobre gold.obt_voos
Fase 4: governança e regras de negócio aplicadas a um LLM

Este agente:
1. Converte perguntas em linguagem natural em consultas SQL válidas para o Databricks.
2. Instrui o modelo com o contrato de dados e as 6 regras de negócio (R1 a R6) do projeto.
3. Valida a segurança do SQL (somente SELECT/WITH, bloqueio de DDL/DML, injeção de LIMIT).
4. Executa a query contra o Databricks SQL Warehouse (via databricks-sql-connector).
5. Monta a resposta em português com os números do Databricks e as regras que o SQL usou
   (uma única chamada ao Gemini por pergunta: a que escreve o SQL).
"""

import os
import re
import json
import sys
from pathlib import Path
from dotenv import load_dotenv

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Carrega variáveis de ambiente do arquivo .env (na pasta agente_ia ou na raiz)
env_path = Path(__file__).parent / '.env'
if not env_path.exists():
    env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Caminho para o contrato de colunas (gerado a partir de 03_gold.py)
CONTRATO_PATH = Path(__file__).parent / 'contrato_obt.json'


def carregar_contrato():
    """Carrega o contrato com os comentários de todas as 62 colunas da gold.obt_voos."""
    if CONTRATO_PATH.exists():
        with open(CONTRATO_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


def construir_system_prompt():
    """Gera o System Prompt dinamicamente a partir do contrato de dados e das regras autorais."""
    contrato = carregar_contrato()
    catalogo = os.getenv("DATABRICKS_CATALOG", "voebem")
    tabela = f"{catalogo}.gold.obt_voos"

    linhas_contrato = []
    for item in contrato:
        col = item["coluna"]
        cmt = item["comentario"]
        linhas_contrato.append(f"- `{col}`: {cmt}")

    contrato_texto = "\n".join(linhas_contrato)

    prompt = f"""Você é o especialista em dados analíticos do projeto VoeBem (Imersão Dados ANAC/Alura).
Sua responsabilidade é converter perguntas de usuários em consultas SQL exatas para o Databricks SQL.

A fonte primária de dados é a tabela OBT desnormalizada: `{tabela}`.
Esta tabela possui ~1 milhão de registros e representa 12 meses de dados abertos da ANAC (VRA: ago/2025 a jul/2026).

---
### CONTRATO DE DADOS DE {tabela} (62 COLUNAS):
{contrato_texto}

---
### REGRAS DE GOVERNANÇA E NEGÓCIO MANDATÓRIAS (AUTORAIS DO VOEBEM):

1. **REGRA R1 — Voo Programado e Denominadores de Cálculo:**
   - Para cálculo de **Cancelamento**: use SEMPRE `WHERE entra_em_cancelamento = true`. Voo com código DI ≠ 0 ou 2 não é programado e possui cancelamento 0,00% por definição.
   - Para cálculo de **Pontualidade**: use SEMPRE `WHERE entra_em_pontualidade = true`.

2. **REGRA R2 — Voos-Fantasma e Cancelamento Operacional (CRÍTICO):**
   - Na base, 23,4% dos cancelamentos brutos vêm de 54 pares (companhia, número do voo) com 30 ou mais ocorrências e 98% ou mais delas canceladas (voos-fantasma).
   - Ao calcular a taxa de cancelamento de companhias ou da malha geral, use a coluna `cancelamento_operacional` e TIRE os voos-fantasma do denominador também (`AND NOT voo_fantasma` no WHERE). Contar as linhas de voo-fantasma como "não canceladas" reduz a taxa pela metade e está ERRADO.
   - Exemplo da fórmula correta:
     `SELECT ROUND(100.0 * AVG(CASE WHEN cancelamento_operacional THEN 1.0 ELSE 0 END), 2) AS taxa_cancelamento FROM ... WHERE entra_em_cancelamento AND NOT voo_fantasma AND icao_empresa = 'IBE'`
   - Use a coluna bruta `voo_cancelado` APENAS SE o usuário solicitar explicitamente "taxa bruta", "sem filtro", "declarado pela companhia" ou "sem tratar fantasmas".

3. **REGRA R3 — Fuso Horário Local vs Hora de Brasília:**
   - Horários de pico de UM aeroporto específico (ex: Manaus, Rio Branco, Porto Velho) devem usar a coluna `hora_local_origem` (e `partida_prevista_local`), NÃO `hora_brasilia`.
   - Use `hora_brasilia` apenas se a pergunta for sobre efeito cascata, rede nacional ou horário sincronizado de Brasília.

4. **REGRA R4 — Suspeita de Erro Físico e Duplicatas:**
   - `duplicata_de_origem = true`: segunda ocorrência dos 42 pares de linhas idênticas publicadas pela ANAC. Já é excluída automaticamente pelo filtro `entra_em_cancelamento` e `entra_em_pontualidade`.
   - `suspeita_erro_horario = true`: velocidade média fora de 200–950 km/h (7.532 voos).

5. **REGRA R5 — Dias Atípicos:**
   - `dia_atipico = true`: dias em que o cancelamento nacional passou de 3x a mediana diária do período.

6. **REGRA R6 — A Média Esconde o p90:**
   - Ao analisar atrasos de rotas, calcule além da média o percentil 90 (`percentile_approx(atraso_chegada_min, 0.90)` ou `percentile_approx(atraso_partida_min, 0.90)`).

---
### INSTRUÇÕES ESTRITAS DE SAÍDA:
Responda com exatamente duas partes e nada mais:

1. O bloco SQL, entre ```sql e ```.
   - A query DEVE começar com `SELECT` ou `WITH`.
   - NUNCA inclua comandos de modificação (DROP, DELETE, INSERT, UPDATE, ALTER, TRUNCATE, CREATE).
   - Use `ROUND(..., 2)` para percentuais e médias.
   - Ordene o resultado (ORDER BY ... DESC/ASC) para que a PRIMEIRA linha seja a resposta da pergunta
     (ex.: horário de pico → a hora com mais partidas primeiro).
   - Dê nomes simples, em minúsculas e sem acento, às colunas calculadas (ex.: `taxa_cancelamento`, `hora`, `partidas`).

2. Uma linha começando com `RESPOSTA:` com UMA frase curta em português que responde a pergunta,
   usando marcadores `{{nome_da_coluna}}` no lugar dos valores. O código troca cada marcador pelo valor
   da primeira linha do resultado.
   - NUNCA escreva números na frase: todo número vem de um marcador.
   - Escreva a unidade depois do marcador quando fizer sentido (`{{taxa_cancelamento}}%`, `{{hora}}h`, `{{atraso_medio}} min`).
   - Exemplos:
     RESPOSTA: A taxa de cancelamento da Iberia no período é de {{taxa_cancelamento}}%.
     RESPOSTA: O horário de pico de partidas em Manaus é às {{hora}}h (hora local), com {{partidas}} partidas.
"""
    return prompt


MODELO_PADRAO = "gemini-3.7-flash"
# Se o modelo principal estiver sobrecarregado (erro temporário do Google), o agente faz UMA
# segunda tentativa com este modelo. Pergunta normal = 1 chamada; no pior caso, 2.
MODELO_RESERVA = "gemini-3.5-flash"
CODIGOS_TEMPORARIOS = {429, 500, 502, 503, 504}


class GeminiIndisponivel(RuntimeError):
    """O Gemini continuou indisponível também no modelo reserva."""


def _erro_temporario(e: Exception) -> bool:
    codigo = getattr(e, "code", None) or getattr(e, "status_code", None)
    if codigo in CODIGOS_TEMPORARIOS:
        return True
    texto = str(e)
    return any(m in texto for m in ("UNAVAILABLE", "RESOURCE_EXHAUSTED", "overloaded", "high demand"))


def _gerar_texto(prompt: str, system: str | None = None) -> str:
    """Única porta de saída para o Gemini. Temperatura 0 para reduzir a variação entre respostas."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY não encontrada. Preencha agente_ia/.env.")
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        raise ImportError("Execute: pip install google-genai")

    client = genai.Client(api_key=api_key)
    principal = os.getenv("GEMINI_MODEL", MODELO_PADRAO)
    reserva = os.getenv("GEMINI_MODEL_RESERVA", MODELO_RESERVA)
    modelos = [principal] if reserva in ("", principal) else [principal, reserva]

    for i, modelo in enumerate(modelos):
        try:
            resposta = client.models.generate_content(
                model=modelo,
                contents=prompt,
                config=types.GenerateContentConfig(system_instruction=system, temperature=0),
            )
            return (resposta.text or "").strip()
        except Exception as e:
            if not _erro_temporario(e):
                raise
            if i == len(modelos) - 1:
                raise GeminiIndisponivel(
                    "O Gemini está sobrecarregado agora (erro temporário do Google, modelo "
                    + " e depois ".join(modelos)
                    + "). Espere um ou dois minutos e pergunte de novo."
                ) from e


def validar_sql(query: str) -> tuple[bool, str]:
    """
    Valida a consulta SQL gerada:
    1. Remove cercas de markdown se existirem.
    2. strip() e remove um ';' final se houver.
    3. Se ainda sobrar ';' -> rejeita: múltiplas instruções não são permitidas.
    4. Confirma se inicia com SELECT ou WITH.
    5. Bloqueia comandos perigosos DDL/DML.
    6. Rejeita referência a information_schema ou a system.
    7. Garante LIMIT 500 ancorado no fim.
    """
    cleaned = query.strip()
    # 1. Remove cercas de markdown se existirem
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:sql)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        cleaned = cleaned.strip()

    # 2. strip() e remover UM ';' final se houver
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()

    # 3. Se ainda sobrar ';' -> rejeitar
    if ";" in cleaned:
        return False, "Múltiplas instruções não são permitidas no SQL."

    # 4. Confirma se inicia com SELECT ou WITH
    if not re.match(r"^(SELECT|WITH)\b", cleaned, re.IGNORECASE):
        return False, "A consulta gerada não é de leitura (deve iniciar com SELECT ou WITH)."

    # 5. Bloqueia palavras proibidas (palavra inteira, case-insensitive)
    proibidas = r"\b(DROP|DELETE|INSERT|UPDATE|ALTER|TRUNCATE|CREATE|MERGE|GRANT|REVOKE|COPY|VACUUM|OPTIMIZE|CALL|USE)\b"
    match_proibida = re.search(proibidas, cleaned, re.IGNORECASE)
    if match_proibida:
        return False, f"Comando proibido detectado no SQL: {match_proibida.group(1)}."

    # 6. Rejeita referência a information_schema ou a system.
    if re.search(r"(\binformation_schema\b|\bsystem\.)", cleaned, re.IGNORECASE):
        return False, "Acesso a catálogos de sistema ou information_schema não é permitido."

    # 7. LIMIT: se não termina em LIMIT <n>, acrescentar \nLIMIT 500. Se termina com LIMIT n e n > 500, trocar por 500.
    match_limit = re.search(r"\bLIMIT\s+(\d+)\s*$", cleaned, re.IGNORECASE)
    if match_limit:
        val = int(match_limit.group(1))
        if val > 500:
            cleaned = cleaned[:match_limit.start()] + "LIMIT 500"
    else:
        cleaned = cleaned + "\nLIMIT 500"

    return True, cleaned


def gerar_sql_e_frase(pergunta: str) -> tuple[str, str]:
    """Uma única chamada ao Gemini: devolve o SQL e a frase-modelo da resposta (com marcadores)."""
    system_prompt = construir_system_prompt()
    full_prompt = f"Pergunta do usuário: {pergunta}"
    bruto = _gerar_texto(prompt=full_prompt, system=system_prompt)

    frase = ""
    m = re.search(r"^\s*RESPOSTA:\s*(.+)$", bruto, re.MULTILINE)
    if m:
        frase = m.group(1).strip()
        bruto = bruto[:m.start()] + bruto[m.end():]
    return _limpar_sql(bruto), frase


def gerar_sql_com_gemini(pergunta: str) -> str:
    """Só o SQL (usado pelo teste de aceitação)."""
    return gerar_sql_e_frase(pergunta)[0]


def _limpar_sql(raw_sql: str) -> str:

    # Limpeza de markdown
    if "```sql" in raw_sql:
        raw_sql = raw_sql.split("```sql")[1].split("```")[0].strip()
    elif "```" in raw_sql:
        raw_sql = raw_sql.split("```")[1].split("```")[0].strip()

    return raw_sql


def executar_no_databricks(query_sql: str) -> tuple[list[str], list[tuple]]:
    """
    Executa a query contra o Databricks SQL Warehouse usando databricks-sql-connector.
    """
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")

    if not all([server_hostname, http_path, access_token]):
        raise ConnectionError(
            "Credenciais do Databricks incompletas em agente_ia/.env.\n"
            "Verifique DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH e DATABRICKS_TOKEN."
        )

    try:
        from databricks import sql
    except ImportError:
        raise ImportError("Execute: pip install databricks-sql-connector")

    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query_sql)
            colunas = [desc[0] for desc in cursor.description]
            resultados = cursor.fetchmany(500)
            return colunas, resultados


# Regras que o SQL gerado usou, detectadas pelo nome das colunas (sem chamar o Gemini de novo).
REGRAS_NO_SQL = [
    (r"\bentra_em_(cancelamento|pontualidade)\b", "R1: só os voos elegíveis no denominador"),
    (r"\bcancelamento_operacional\b", "R2: cancelamento sem os voos-fantasma"),
    (r"\bvoo_cancelado\b", "taxa bruta (inclui os voos-fantasma)"),
    (r"\bhora_local_origem\b", "R3: hora local do aeroporto"),
    (r"\bsuspeita_erro_horario\b", "R4: horário implausível"),
    (r"\bdia_atipico\b", "R5: dia atípico"),
    (r"\bpercentile", "R6: P90 do atraso"),
]


def _formatar(valor) -> str:
    """Número no formato brasileiro (1.234,56); o resto como texto."""
    from decimal import Decimal
    if valor is None:
        return "—"
    if isinstance(valor, bool):
        return "sim" if valor else "não"
    if isinstance(valor, int):
        return f"{valor:,}".replace(",", ".")
    if isinstance(valor, (float, Decimal)):
        v = float(valor)
        casas = 0 if v.is_integer() else 2
        return f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return str(valor)


def montar_resposta(query_sql: str, colunas: list[str], resultados: list[tuple]) -> str:
    """Resposta em português montada pelo código a partir do resultado do Databricks.
    Não chama o Gemini: cada pergunta gasta uma única chamada (a que escreve o SQL),
    e os números mostrados são exatamente os que o Databricks devolveu."""
    if not resultados:
        texto = "A consulta não retornou nenhuma linha."
    elif len(resultados) == 1:
        partes = [f"{c}: {_formatar(v)}" for c, v in zip(colunas, resultados[0])]
        texto = "Resultado — " + "; ".join(partes) + "."
    else:
        mostrar = resultados[:5]
        linhas = [" · ".join(f"{c}: {_formatar(v)}" for c, v in zip(colunas, r)) for r in mostrar]
        texto = f"A consulta retornou {len(resultados)} linhas"
        texto += (f". As {len(mostrar)} primeiras:\n" if len(resultados) > len(mostrar) else ":\n")
        texto += "\n".join("• " + l for l in linhas)

    return texto


def regras_do_sql(query_sql: str) -> list[str]:
    """Regras do projeto que aparecem no SQL executado (pelo nome das colunas)."""
    return [nome for padrao, nome in REGRAS_NO_SQL if re.search(padrao, query_sql, re.IGNORECASE)]


def preencher_frase(frase: str, colunas: list[str], resultados: list[tuple]) -> str | None:
    """Troca cada {coluna} da frase do Gemini pelo valor da 1ª linha do resultado.
    Devolve None (e o código usa a resposta em lista) se a frase não servir:
    sem resultado, marcador que não existe, ou algum número escrito pelo próprio Gemini."""
    if not frase or not resultados:
        return None
    marcadores = re.findall(r"\{([^{}]+)\}", frase)
    if not marcadores:
        return None
    # Número só pode vir do dado: tirando os marcadores e nomes de regra (R1..R6), não pode sobrar dígito.
    if re.search(r"\d", re.sub(r"\bR[1-6]\b", "", re.sub(r"\{[^{}]+\}", "", frase))):
        return None
    valores = {c.lower(): v for c, v in zip(colunas, resultados[0])}
    for mk in marcadores:
        if mk.strip().lower() not in valores:
            return None
    return re.sub(r"\{([^{}]+)\}", lambda m: _formatar(valores[m.group(1).strip().lower()]), frase)


def perguntar(pergunta: str, verbose: bool = True) -> dict:
    """
    Fluxo completo de ponta a ponta:
    Pergunta -> Geração SQL (1 chamada ao Gemini) -> Validação -> Execução no Databricks -> Resposta.
    """
    if verbose:
        print(f"\n💬 Pergunta: \"{pergunta}\"")

    # 1. Gerar SQL (e a frase-modelo da resposta) com UMA chamada ao Gemini
    sql_bruto, frase = gerar_sql_e_frase(pergunta)
    valido, sql_validado = validar_sql(sql_bruto)

    if not valido:
        raise ValueError(f"SQL rejeitado pela validação de segurança: {sql_validado}")

    if verbose:
        print("\n🔍 SQL Gerado:")
        print("--------------------------------------------------")
        print(sql_validado)
        print("--------------------------------------------------")

    # 2. Executar no Databricks
    colunas, linhas = executar_no_databricks(sql_validado)

    if verbose:
        print(f"\n📊 Resultado do Databricks ({len(linhas)} linha(s)):")
        try:
            from tabulate import tabulate
            print(tabulate(linhas[:15], headers=colunas, tablefmt="psql"))
        except ImportError:
            print(colunas)
            for r in linhas[:10]:
                print(r)

    # 3. Resposta montada pelo código (sem segunda chamada ao Gemini)
    resposta = preencher_frase(frase, colunas, linhas) or montar_resposta(sql_validado, colunas, linhas)
    regras = regras_do_sql(sql_validado)

    if verbose:
        print("\n🤖 Resposta:")
        print(resposta)
        if regras:
            print("Regras usadas no SQL: " + "; ".join(regras))
        print("==================================================\n")

    return {
        "pergunta": pergunta,
        "sql": sql_validado,
        "colunas": colunas,
        "linhas": linhas,
        "resposta": resposta,
        "regras": regras,
    }


if __name__ == "__main__":
    import sys
    print("=" * 65)
    print("✈️  VoeBem — Agente de Governança e IA (Texto -> SQL)")
    print("=" * 65)

    if len(sys.argv) > 1:
        pergunta_cli = " ".join(sys.argv[1:])
        perguntar(pergunta_cli)
    else:
        print("\nModo interativo iniciado. Digite sua pergunta ou 'sair':")
        while True:
            try:
                entrada = input("\nPergunta > ").strip()
                if not entrada:
                    continue
                if entrada.lower() in ("sair", "exit", "quit"):
                    print("Encerrando agente. Bons voos!")
                    break
                perguntar(entrada)
            except KeyboardInterrupt:
                print("\nEncerrando agente.")
                break
            except Exception as e:
                print(f"\n❌ Erro: {e}")
