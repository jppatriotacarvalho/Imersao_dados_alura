# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — espelho governado do bronze
# MAGIC
# MAGIC A regra da casa, e ela não é negociável:
# MAGIC
# MAGIC > **A silver é o espelho do bronze com governança aplicada.**
# MAGIC > Mesmo grão, **mesma contagem de linhas**.
# MAGIC
# MAGIC | | permitido | proibido |
# MAGIC |---|---|---|
# MAGIC | tipagem (`string` → `TIMESTAMP`, `INT`, `DOUBLE`) | ✅ | |
# MAGIC | legibilidade (quebrar timestamp em data e hora) | ✅ | |
# MAGIC | renomear coluna para dizer a verdade | ✅ | |
# MAGIC | unificar dois cadastros do mesmo assunto, preservando a origem | ✅ | |
# MAGIC | aritmética pura (`atraso = real − previsto`) | ✅ | |
# MAGIC | chave técnica determinística (hash) | ✅ | |
# MAGIC | filtro / `WHERE` de negócio | | ❌ |
# MAGIC | `GROUP BY` / agregação materializada | | ❌ |
# MAGIC | limiar, flag, classificação | | ❌ |
# MAGIC
# MAGIC **Por quê?** Porque a silver precisa servir várias análises, e toda linha que ela descarta é uma
# MAGIC pergunta que ninguém mais vai conseguir fazer. Filtro fecha porta.
# MAGIC
# MAGIC O teste para qualquer coluna nova: *isso embute uma decisão de negócio?*
# MAGIC `atraso = real − previsto` é subtração → silver.
# MAGIC `pontual = atraso <= 15` embute o número **15**, que muda por cliente → gold.
# MAGIC
# MAGIC Três acréscimos em relação ao roteiro padrão, e cada um passa nesse teste:
# MAGIC **renomear os horários para `_brasilia`**, **`_id_etapa` por hash** e **testes de contrato**.

# COMMAND ----------

from pyspark.sql import functions as F
CATALOGO = "voebem"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Medir antes de escrever o `CAST`
# MAGIC
# MAGIC Duas armadilhas escondidas no bronze:

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      COUNT(*)                                                       AS linhas,
      SUM(CASE WHEN partida_real IS NULL OR partida_real = ''
                 OR lower(partida_real) = 'null' THEN 1 ELSE 0 END)  AS partida_real_ausente,
      SUM(CASE WHEN lower(partida_real) = 'null' THEN 1 ELSE 0 END)  AS ausencia_como_texto,
      SUM(CASE WHEN partida_prevista LIKE '%.%' THEN 1 ELSE 0 END)   AS com_fracao_de_segundo,
      SUM(CASE WHEN _marcador_registro THEN 1 ELSE 0 END)            AS com_marcador_preservado
    FROM {CATALOGO}.bronze.vra
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Armadilha 1 — a ausência pode vir como a string `'null'`**, quatro caracteres de texto.
# MAGIC `WHERE partida_real IS NULL` devolve **zero** numa tabela onde 29 mil voos não têm horário real.
# MAGIC Correção: `nullif` **antes** do cast. (Dependendo do snapshot da ANAC a ausência vem como campo
# MAGIC vazio; tratamos os dois casos, custa nada.)
# MAGIC
# MAGIC **Armadilha 2 — dois formatos de timestamp no mesmo arquivo.** A maioria vem
# MAGIC `2026-01-27 19:45:00`, mas ~80 mil linhas vêm com fração de segundo. Um `to_timestamp` com máscara
# MAGIC fixa devolveria NULL para 8% da base, **em silêncio**. O `try_cast` aceita os dois formatos, e o
# MAGIC `try_` garante que um formato novo vire NULL em vez de derrubar o job.
# MAGIC
# MAGIC Isso é **tipagem**, não limpeza de negócio: `'null'` é como a fonte escreve "ausente". Traduzir
# MAGIC para `NULL` é dizer a mesma coisa no tipo certo. Nenhuma linha sai.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. O nome das colunas de tempo precisa mudar
# MAGIC
# MAGIC Os horários do VRA **não estão na hora local de cada aeroporto**. Estão todos num relógio só:
# MAGIC hora de Brasília (UTC−3). Duas provas independentes, medidas neste dataset:
# MAGIC
# MAGIC **(a) Ida e volta têm a mesma duração em rota que cruza fuso.**
# MAGIC
# MAGIC | rota | ida | volta | seria, se cada ponta tivesse relógio próprio |
# MAGIC |---|---|---|---|
# MAGIC | GRU ↔ MAO (−3/−4) | 235 min | 235 min | 175 e 295 |
# MAGIC | GRU ↔ CGB (−3/−4) | 135 min | 135 min | 75 e 195 |
# MAGIC | GRU ↔ MAD (−3/+2) | 610 min | 635 min | ~910 e ~310 |
# MAGIC
# MAGIC Iguais nos dois sentidos ⇒ existe **um relógio único** para o arquivo inteiro.
# MAGIC
# MAGIC **(b) Qual relógio é esse?** Congonhas tem restrição noturna definida em hora **local** (23h–06h).
# MAGIC No relógio do arquivo o zero está exatamente em **23h–05h59**, com 6.991 partidas às 06h. Se o
# MAGIC arquivo fosse UTC, o mesmo fechamento apareceria em 02h–09h — e haveria 6.991 partidas às 3h da
# MAGIC manhã local, durante o fechamento. Não acontece.
# MAGIC
# MAGIC Consequência medida: **79 dos 496 aeródromos** do cadastro estão fora do UTC−3 (entre os 396
# MAGIC aeroportos do VRA, são 34), e deles partem **42.235 etapas** (4,8% das etapas com fuso conhecido)
# MAGIC cuja hora no dado está 1h ou 2h deslocada da hora local. O pico de Manaus vai de 12h para
# MAGIC 11h; o de Rio Branco, de 02h para 00h.
# MAGIC
# MAGIC Então a coluna passa a se chamar `partida_prevista_brasilia`. É o mesmo movimento que se faz
# MAGIC quando a coluna `UF` do cadastro contém `"São Paulo"` e não `"SP"`: **renomear e documentar é
# MAGIC governança; fingir que está certo é dívida.** Converter para hora local exige tabela de fuso —
# MAGIC isso é decisão de modelagem e mora na gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `silver.vra` — o espelho
# MAGIC
# MAGIC Repare no que **não** existe nesta query: nenhum `WHERE`, nenhum `GROUP BY`, nenhum `DISTINCT`,
# MAGIC nenhum `JOIN`. É um `SELECT` de projeção sobre o bronze inteiro.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.silver.vra AS
WITH tipado AS (
  SELECT
    icao_empresa,
    numero_voo,
    codigo_di,
    codigo_tipo_linha,
    icao_origem,
    icao_destino,
    try_cast(nullif(nullif(partida_prevista, 'null'), '') AS TIMESTAMP) AS partida_prevista_brasilia,
    try_cast(nullif(nullif(partida_real,     'null'), '') AS TIMESTAMP) AS partida_real_brasilia,
    try_cast(nullif(nullif(chegada_prevista, 'null'), '') AS TIMESTAMP) AS chegada_prevista_brasilia,
    try_cast(nullif(nullif(chegada_real,     'null'), '') AS TIMESTAMP) AS chegada_real_brasilia,
    situacao_voo,
    nullif(nullif(codigo_justificativa, 'N/A'), '')                     AS codigo_justificativa,
    _marcador_registro,
    _arquivo_origem,
    _fonte_atualizada_em,
    _ingerido_em
  FROM {CATALOGO}.bronze.vra
),
projetado AS (
SELECT
  -- ---------- hash do conteudo da etapa ----------
  -- NAO e unico: a fonte da ANAC repete 42 pares de linhas identicas. Ver secao 4.
  sha2(concat_ws('|',
        icao_empresa, numero_voo, codigo_di, icao_origem, icao_destino,
        coalesce(cast(partida_prevista_brasilia AS STRING), 'SEM_PREVISTO'),
        coalesce(cast(partida_real_brasilia     AS STRING), 'SEM_REAL')
      ), 256)                                                AS _hash_etapa,

  icao_empresa,
  numero_voo,
  codigo_di,
  codigo_tipo_linha,
  icao_origem,
  icao_destino,

  -- ---------- tempo (hora de Brasilia, UTC-3) ----------
  partida_prevista_brasilia,
  CAST(partida_prevista_brasilia AS DATE)                    AS partida_prevista_data,
  date_format(partida_prevista_brasilia, 'HH:mm')            AS partida_prevista_hora,

  partida_real_brasilia,
  CAST(partida_real_brasilia AS DATE)                        AS partida_real_data,
  date_format(partida_real_brasilia, 'HH:mm')                AS partida_real_hora,

  chegada_prevista_brasilia,
  CAST(chegada_prevista_brasilia AS DATE)                    AS chegada_prevista_data,
  date_format(chegada_prevista_brasilia, 'HH:mm')            AS chegada_prevista_hora,

  chegada_real_brasilia,
  CAST(chegada_real_brasilia AS DATE)                        AS chegada_real_data,
  date_format(chegada_real_brasilia, 'HH:mm')                AS chegada_real_hora,

  situacao_voo,
  codigo_justificativa,

  -- ---------- aritmetica pura: subtracao, sem limiar e sem decisao ----------
  CAST(timestampdiff(MINUTE, partida_prevista_brasilia, partida_real_brasilia) AS INT) AS atraso_partida_min,
  CAST(timestampdiff(MINUTE, chegada_prevista_brasilia, chegada_real_brasilia) AS INT) AS atraso_chegada_min,
  CAST(timestampdiff(MINUTE, partida_prevista_brasilia, partida_real_brasilia)
     - timestampdiff(MINUTE, chegada_prevista_brasilia, chegada_real_brasilia) AS INT) AS minutos_recuperados,

  -- duracao: mesma natureza dos atrasos, e e o que permite checar plausibilidade fisica na gold
  CAST(timestampdiff(MINUTE, partida_prevista_brasilia, chegada_prevista_brasilia) AS INT) AS duracao_programada_min,
  CAST(timestampdiff(MINUTE, partida_real_brasilia,     chegada_real_brasilia)     AS INT) AS duracao_real_min,

  _marcador_registro,
  _arquivo_origem,
  _fonte_atualizada_em,
  _ingerido_em,
  current_timestamp()                                        AS _transformado_em
FROM tipado
),
numerado AS (
  -- ordinal dentro do grupo de linhas identicas: 1 para a primeira, 2 para a repetida.
  -- E identidade posicional, nao julgamento de negocio - decidir se a 2a conta como voo
  -- e regra de negocio, e isso mora na gold.
  SELECT *,
         CAST(row_number() OVER (PARTITION BY _hash_etapa
                                 ORDER BY _arquivo_origem, _ingerido_em) AS INT) AS _ocorrencia
  FROM projetado
)
SELECT
  concat(_hash_etapa, '#', _ocorrencia)                      AS _id_etapa,
  *
FROM numerado
""")

print("silver.vra criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Por que a chave técnica cabe na silver — e por que ela tem duas partes
# MAGIC
# MAGIC Um hash determinístico sobre as colunas que identificam a etapa não filtra, não agrega e não
# MAGIC embute limiar nenhum. É **identidade**, não regra de negócio — a mesma linha gera sempre o mesmo
# MAGIC hash, em qualquer execução, em qualquer máquina.
# MAGIC
# MAGIC E é o que torna possível a carga incremental no banco lá na frente. Sem ele, o `MERGE` precisa de
# MAGIC chave de negócio — e a chave de negócio do VRA **não é única**: medimos 137 colisões de
# MAGIC `(empresa, voo, origem, destino, partida_prevista)` com horários reais diferentes. Exemplo real:
# MAGIC
# MAGIC ```
# MAGIC AAL 0905 KMIA→SBGL previsto 2026-05-21 23:55  real 21/05 02:02  chegou 10:02
# MAGIC AAL 0905 KMIA→SBGL previsto 2026-05-21 23:55  real 22/05 10:42  chegou 18:37
# MAGIC ```
# MAGIC
# MAGIC Incluir o horário real no hash separa essas duas. **Mas o hash sozinho ainda não é único** — e
# MAGIC descobrir isso foi o contrato funcionando:
# MAGIC
# MAGIC ```
# MAGIC [FALHA] _id_etapa duplicado: 42 (esperado 0)
# MAGIC ```
# MAGIC
# MAGIC Os **42 pares de linhas repetidas** (mesmo hash de conteúdo) que a ANAC publica. Não é colisão de hash (a
# MAGIC chance em SHA-256 é astronômica) nem erro do pipeline: são registros repetidos na origem.
# MAGIC
# MAGIC ```
# MAGIC QTR 8172  SBGR→SKBO  07/12 15:15  real 15:07  chegou 20:42  REALIZADO
# MAGIC QTR 8172  SBGR→SKBO  07/12 15:15  real 15:07  chegou 20:42  REALIZADO   <- byte a byte igual
# MAGIC ```
# MAGIC
# MAGIC 41 dos 42 pares são idênticos em **todas** as colunas de negócio; 1 difere em algo fora do hash.
# MAGIC 40 pares estão dentro do mesmo arquivo mensal, 2 cruzam arquivos. Concentram-se em cargueiras e
# MAGIC internacionais (ITA 42 linhas, Qatar 14, Emirates 12, Cargolux 10).
# MAGIC
# MAGIC **A solução não é apagar a linha.** A silver é espelho: se ela sumir aqui, a contagem quebra e o
# MAGIC próprio achado desaparece. A chave técnica ganha duas partes:
# MAGIC
# MAGIC | coluna | o que é |
# MAGIC |---|---|
# MAGIC | `_hash_etapa` | o hash do conteúdo. **Duplicado de propósito**: é o que identifica "a mesma etapa" |
# MAGIC | `_ocorrencia` | ordinal dentro do grupo: 1 para a primeira, 2 para a repetida |
# MAGIC | `_id_etapa` | `hash#ocorrencia` — agora **único**, e é a chave do `MERGE` |
# MAGIC
# MAGIC Nota honesta sobre o ordinal: entre duas linhas rigorosamente idênticas, quem recebe `#1` e quem
# MAGIC recebe `#2` é arbitrário. O que importa para o `MERGE` é que o **conjunto** de ids seja estável a
# MAGIC cada execução — e é, porque a partição e a ordenação são determinísticas.
# MAGIC
# MAGIC E a decisão de **contar ou não** a segunda ocorrência como voo? Isso é julgamento de negócio.
# MAGIC Fica na gold, junto com as outras regras.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. A prova que importa: mesma contagem
# MAGIC
# MAGIC Este é o critério objetivo do marco. Se a diferença não for **zero**, a silver não é espelho —
# MAGIC é recorte, e alguém em algum momento vai fazer uma pergunta que ela não consegue mais responder.

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      (SELECT COUNT(*) FROM {CATALOGO}.bronze.vra) AS bronze,
      (SELECT COUNT(*) FROM {CATALOGO}.silver.vra) AS silver,
      (SELECT COUNT(*) FROM {CATALOGO}.bronze.vra)
        - (SELECT COUNT(*) FROM {CATALOGO}.silver.vra) AS diferenca
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(partida_prevista_brasilia) AS partida_prevista_ok,
           COUNT(partida_real_brasilia)     AS partida_real_ok,
           COUNT(chegada_prevista_brasilia) AS chegada_prevista_ok,
           COUNT(chegada_real_brasilia)     AS chegada_real_ok,
           COUNT(atraso_partida_min)        AS atraso_partida_ok,
           COUNT(duracao_programada_min)    AS duracao_programada_ok
    FROM {CATALOGO}.silver.vra
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Testes de contrato
# MAGIC
# MAGIC "Mesma contagem" é o teste clássico, e é necessário. **Não é suficiente:** uma tabela pode ter a
# MAGIC contagem certa e o conteúdo errado. Seis contratos — os fatais param o job, os de aviso só
# MAGIC registram. Foi um deles que descobriu as duplicatas da fonte.

# COMMAND ----------

def contrato(nome, sql, esperado=0, fatal=True):
    valor = spark.sql(sql).collect()[0][0]
    ok = (valor == esperado)
    print(f"[{'OK ' if ok else 'FALHA'}] {nome}: {valor:,} (esperado {esperado})")
    if not ok and fatal:
        raise AssertionError(f"Contrato violado: {nome} = {valor}, esperado {esperado}")
    return valor


# 1. espelho: mesma contagem
contrato("contagem bronze == silver",
    f"SELECT (SELECT COUNT(*) FROM {CATALOGO}.bronze.vra) - (SELECT COUNT(*) FROM {CATALOGO}.silver.vra)")

# 2. a chave tecnica e unica (se nao for, o MERGE no banco quebra) - AGORA PASSA
contrato("_id_etapa duplicado",
    f"SELECT COUNT(*) FROM (SELECT _id_etapa FROM {CATALOGO}.silver.vra GROUP BY _id_etapa HAVING COUNT(*) > 1)")

# 2b. o hash do conteudo NAO e unico, e isso e a fonte repetindo linha - AVISO, nao falha
contrato("linhas rigorosamente identicas na origem",
    f"SELECT COUNT(*) FROM (SELECT _hash_etapa FROM {CATALOGO}.silver.vra GROUP BY _hash_etapa HAVING COUNT(*) > 1)",
    fatal=False)

# 3. dominio fechado de situacao_voo
contrato("situacao_voo fora do dominio",
    f"SELECT COUNT(*) FROM {CATALOGO}.silver.vra WHERE situacao_voo NOT IN ('REALIZADO','CANCELADO')")

# 4. tipagem: chegada programada nunca antes da partida programada
contrato("chegada programada antes da partida",
    f"""SELECT COUNT(*) FROM {CATALOGO}.silver.vra
        WHERE duracao_programada_min IS NOT NULL AND duracao_programada_min < 0""")

# 5. codigo novo na fonte - AVISO, nao falha: a seed e que precisa crescer
contrato("codigo_di ausente da tabela de referencia",
    f"""SELECT COUNT(DISTINCT v.codigo_di) FROM {CATALOGO}.silver.vra v
        LEFT JOIN {CATALOGO}.ref.codigos_operacao r
          ON r.dominio = 'codigo_di' AND r.codigo = v.codigo_di
        WHERE r.codigo IS NULL""", fatal=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Os dois contratos que descobriram coisa
# MAGIC
# MAGIC O contrato 2b não falha o job, mas registra o achado. E se você trocar `_id_etapa` pela **chave
# MAGIC de negócio** no contrato 2, ele quebra com 137 linhas — outro achado.
# MAGIC
# MAGIC Nenhum dos dois é bug do teste. A diferença entre um pipeline que descobre isso agora e um que
# MAGIC descobre em produção é este bloco de cinco linhas.

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'chave de negocio (empresa+voo+rota+previsto)' AS chave, COUNT(*) AS grupos_duplicados
    FROM (
      SELECT icao_empresa, numero_voo, icao_origem, icao_destino, partida_prevista_brasilia
      FROM {CATALOGO}.silver.vra
      WHERE partida_prevista_brasilia IS NOT NULL
      GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1
    )
    UNION ALL
    SELECT 'hash do conteudo (_hash_etapa)', COUNT(*)
    FROM (SELECT _hash_etapa FROM {CATALOGO}.silver.vra GROUP BY _hash_etapa HAVING COUNT(*) > 1)
    UNION ALL
    SELECT 'chave tecnica (_id_etapa)', COUNT(*)
    FROM (SELECT _id_etapa FROM {CATALOGO}.silver.vra GROUP BY _id_etapa HAVING COUNT(*) > 1)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabela de auditoria das duplicatas de origem
# MAGIC
# MAGIC Materializada porque é evidência: alimenta a página de qualidade do dashboard e responde
# MAGIC "quantas linhas do país são repetição da fonte, e de quem".

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.silver.duplicatas_origem AS
SELECT
  _hash_etapa,
  icao_empresa,
  numero_voo,
  codigo_di,
  codigo_tipo_linha,
  icao_origem,
  icao_destino,
  partida_prevista_brasilia,
  situacao_voo,
  COUNT(*)                        AS ocorrencias,
  collect_set(_arquivo_origem)    AS arquivos,
  current_timestamp()             AS _detectado_em
FROM {CATALOGO}.silver.vra
GROUP BY 1,2,3,4,5,6,7,8,9
HAVING COUNT(*) > 1
""")

display(spark.sql(f"""
    SELECT icao_empresa, codigo_tipo_linha,
           COUNT(*)              AS grupos,
           SUM(ocorrencias)      AS linhas,
           SUM(ocorrencias) - COUNT(*) AS linhas_repetidas,
           SUM(CASE WHEN size(arquivos) > 1 THEN 1 ELSE 0 END) AS cruzam_arquivos
    FROM {CATALOGO}.silver.duplicatas_origem
    GROUP BY 1,2 ORDER BY linhas DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT icao_empresa, numero_voo, icao_origem, icao_destino,
           partida_prevista_brasilia, situacao_voo, ocorrencias, arquivos
    FROM {CATALOGO}.silver.duplicatas_origem
    ORDER BY icao_empresa, partida_prevista_brasilia LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `silver.empresas` — o caso clássico dos dois sistemas
# MAGIC
# MAGIC Aqui a silver faz a única coisa que muda a forma da tabela: **unifica dois cadastros do mesmo
# MAGIC assunto**. `bronze.empresas_nacionais` e `bronze.empresas_estrangeiras` são dois processos
# MAGIC administrativos da ANAC descrevendo a mesma entidade de negócio — "empresa aérea que opera no Brasil".
# MAGIC
# MAGIC É permitido porque **não perde informação**: a contagem é a soma exata das duas, e `origem_cadastro`
# MAGIC guarda por registro de onde veio. Quem quiser voltar a olhar só as estrangeiras, consegue.
# MAGIC
# MAGIC O que seria proibido: `WHERE situacao = 'ATIVA'`. Empresa que encerrou operação continua tendo
# MAGIC voado no período — filtrar apagaria o histórico dela.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.silver.empresas AS
SELECT icao, sigla_iata, razao_social, servico, cidade, uf, situacao,
       'nacional' AS origem_cadastro,
       _arquivo_origem, _fonte_atualizada_em, _ingerido_em, current_timestamp() AS _transformado_em
FROM {CATALOGO}.bronze.empresas_nacionais
UNION ALL
SELECT icao, sigla_iata, razao_social, servico, cidade, uf, situacao,
       'estrangeira' AS origem_cadastro,
       _arquivo_origem, _fonte_atualizada_em, _ingerido_em, current_timestamp() AS _transformado_em
FROM {CATALOGO}.bronze.empresas_estrangeiras
""")

contrato("silver.empresas == soma dos dois cadastros",
    f"""SELECT (SELECT COUNT(*) FROM {CATALOGO}.bronze.empresas_nacionais)
             + (SELECT COUNT(*) FROM {CATALOGO}.bronze.empresas_estrangeiras)
             - (SELECT COUNT(*) FROM {CATALOGO}.silver.empresas)""")

contrato("ICAO repetido entre os dois cadastros",
    f"""SELECT COUNT(*) FROM (
          SELECT icao FROM {CATALOGO}.silver.empresas
          WHERE icao IS NOT NULL AND icao <> '' GROUP BY icao HAVING COUNT(*) > 1)""")

display(spark.sql(f"""
    SELECT origem_cadastro, COUNT(*) AS linhas,
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END) AS com_icao
    FROM {CATALOGO}.silver.empresas GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC > Este `GROUP BY` é **conferência**, não construção. A tabela já está escrita; o agrupamento aqui
# MAGIC > só serve para eu olhar o resultado. A proibição vale para o que é **materializado** na silver.
# MAGIC
# MAGIC O segundo contrato acima é o que impede um desastre silencioso: se a mesma empresa aparecesse nos
# MAGIC dois cadastros, o `JOIN` com o VRA **multiplicaria linhas do fato** e a contagem de voos do projeto
# MAGIC subiria sozinha. Hoje passa (167 ICAOs, zero repetidos). Amanhã pode não passar — e aí o job para
# MAGIC em vez de publicar número errado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `silver.aerodromos` e `silver.codigos_operacao` — espelhos
# MAGIC
# MAGIC Duas notas de tipagem no cadastro de aeródromos:
# MAGIC
# MAGIC - `altitude` vem como `"193,0"` — vírgula decimal. Vira `DOUBLE` com um `replace`.
# MAGIC - a coluna que o cabeçalho chama de `UF` contém `"Acre"`, `"São Paulo"`: é o **nome da unidade
# MAGIC   federativa por extenso**, não a sigla. Quem escrever `WHERE uf = 'SP'` recebe zero linhas e vai
# MAGIC   achar que o dado sumiu. O nome da coluna passa a dizer a verdade (`uf_nome`) e o `COMMENT` avisa.
# MAGIC   Renomear e documentar é governança; inventar a sigla seria transformação de negócio.
# MAGIC
# MAGIC E `latitude_decimal` / `longitude_decimal` viram `DOUBLE` — o que destrava o cálculo de distância
# MAGIC de rota na gold.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.silver.aerodromos AS
SELECT
  icao,
  ciad,
  nome,
  municipio,
  uf                                                AS uf_nome,
  municipio_servido,
  uf_servido                                        AS uf_servido_nome,
  try_cast(latitude_decimal  AS DOUBLE)             AS latitude,
  try_cast(longitude_decimal AS DOUBLE)             AS longitude,
  latitude_dms,
  longitude_dms,
  try_cast(replace(altitude, ',', '.') AS DOUBLE)   AS altitude_m,
  operacao_diurna,
  operacao_noturna,
  situacao                                          AS situacao_cadastro,
  validade_registro,
  _arquivo_origem,
  _fonte_atualizada_em,
  _ingerido_em,
  current_timestamp()                               AS _transformado_em
FROM {CATALOGO}.bronze.aerodromos
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.silver.codigos_operacao AS
SELECT dominio, codigo, descricao, catalogado, _fonte_url,
       current_timestamp() AS _transformado_em
FROM {CATALOGO}.ref.codigos_operacao
""")

contrato("ICAO duplicado no cadastro de aerodromos",
    f"""SELECT COUNT(*) FROM (
          SELECT icao FROM {CATALOGO}.silver.aerodromos
          WHERE icao IS NOT NULL AND icao <> '' GROUP BY icao HAVING COUNT(*) > 1)""")

display(spark.sql(f"""
    SELECT 'aerodromos' AS tabela,
           (SELECT COUNT(*) FROM {CATALOGO}.bronze.aerodromos) AS bronze,
           (SELECT COUNT(*) FROM {CATALOGO}.silver.aerodromos) AS silver,
           (SELECT COUNT(latitude) FROM {CATALOGO}.silver.aerodromos) AS com_coordenada,
           (SELECT SUM(CASE WHEN operacao_noturna = 'Sem Operação' THEN 1 ELSE 0 END)
              FROM {CATALOGO}.silver.aerodromos) AS sem_operacao_noturna
    UNION ALL
    SELECT 'codigos_operacao',
           (SELECT COUNT(*) FROM {CATALOGO}.ref.codigos_operacao),
           (SELECT COUNT(*) FROM {CATALOGO}.silver.codigos_operacao), NULL, NULL
"""))

# COMMAND ----------

# MAGIC %md
# COMMAND ----------

# MAGIC %md
# MAGIC ### Um detalhe que derruba o job: aspa simples dentro do comentario
# MAGIC
# MAGIC Varios comentarios citam um valor literal do dado — `'Sem Operacao'`, `DI='D'`, `uf = 'SP'`.
# MAGIC Interpolados direto num `COMMENT '...'`, esses apostrofos **fecham o literal SQL** e o parser
# MAGIC quebra com `PARSE_SYNTAX_ERROR`.
# MAGIC
# MAGIC A tentacao e tirar as aspas do texto. Mas o comentario existe para ser lido por um LLM, e
# MAGIC *"o valor Sem Operacao"* e ambiguo onde *"o valor 'Sem Operacao'"* nao e. A correcao certa e
# MAGIC escapar na hora de montar o SQL: no padrao SQL, `''` dentro de um literal representa um apostrofo.
# MAGIC
# MAGIC Uma funcao, usada em todo `COMMENT` do notebook — e o texto continua dizendo o que precisa dizer.

# COMMAND ----------

def sql_literal(texto: str) -> str:
    """Escapa aspas simples para embutir texto com seguranca num literal SQL."""
    return texto.replace("'", "''")


# MAGIC ## 8. Metadados gerenciados
# MAGIC
# MAGIC Documentação não é enfeite: o consumidor final deste pipeline é um **LLM**, e o `COMMENT` é
# MAGIC literalmente o texto que ele lê para decidir qual coluna usar. Coluna sem comentário é coluna que
# MAGIC a IA vai usar errado — e o erro chega bonito, formatado e com número.
# MAGIC
# MAGIC O comentário descreve **significado de negócio**, não tipo de dado. "TIMESTAMP da partida" não
# MAGIC ajuda ninguém; "horário em que a aeronave efetivamente saiu do solo" ajuda. E quando a coluna tem
# MAGIC uma armadilha, o comentário avisa da armadilha.

# COMMAND ----------

COMENTARIOS_VRA = {
    "_id_etapa":                 "Chave tecnica UNICA da linha: _hash_etapa seguido de # e do numero da ocorrencia. E a chave do MERGE incremental no banco. Existe em duas partes porque a chave de negocio do VRA nao e unica (137 colisoes) e porque nem o hash do conteudo e unico (42 pares de linhas identicas na origem).",
    "_hash_etapa":               "Hash SHA-256 de empresa, voo, DI, origem, destino e os horarios previsto e real. Identifica O CONTEUDO da etapa e NAO e unico de proposito: 42 grupos tem duas linhas rigorosamente identicas, porque a ANAC publica a linha repetida. Ver silver.duplicatas_origem.",
    "_ocorrencia":               "Ordinal da linha dentro do grupo de mesmo _hash_etapa: 1 para a primeira, 2 para a repetida. Identidade posicional, nao julgamento. Entre linhas identicas a atribuicao e arbitraria, mas o conjunto de ids e estavel a cada execucao, que e o que o MERGE exige. A decisao de contar ou nao a segunda ocorrencia como voo e regra de negocio e esta na gold, em duplicata_de_origem.",
    "icao_empresa":              "Codigo ICAO de tres letras da empresa que operou a etapa. Chave para silver.empresas.",
    "numero_voo":                "Numero do voo divulgado pela companhia. Identificador comercial e textual: tem zero a esquerda, pode comecar com letra, e se repete todos os dias. NAO e identificador unico.",
    "codigo_di":                 "Codigo de autorizacao (DI) da etapa. Distingue etapa regular (0) de extra, retorno, charter e duplicada. Descricao em silver.codigos_operacao. Determina se o voo era PROGRAMADO: so DI=0 tem horario previsto, em 100 por cento das linhas.",
    "codigo_tipo_linha":         "Codigo do tipo de linha: N e C domesticas, I e G internacionais. Descricao em silver.codigos_operacao.",
    "icao_origem":               "Codigo ICAO do aerodromo de onde a etapa partiu. Chave para silver.aerodromos - aeroportos estrangeiros nao constam no cadastro da ANAC.",
    "icao_destino":              "Codigo ICAO do aerodromo onde a etapa pousou. Mesma observacao de cobertura da origem.",
    "partida_prevista_brasilia": "Horario de partida programado, em HORA DE BRASILIA (UTC-3) - NAO na hora local do aeroporto. Todos os horarios do VRA usam um relogio unico: verificado por ida e volta terem a mesma duracao em rotas que cruzam fuso. Para hora local do aeroporto use gold.obt_voos.",
    "partida_prevista_data":     "Data da partida programada, hora de Brasilia. Use para serie diaria e recorte de periodo.",
    "partida_prevista_hora":     "Hora e minuto da partida programada (HH:mm), hora de Brasilia.",
    "partida_real_brasilia":     "Horario em que a aeronave efetivamente saiu, hora de Brasilia. Nulo em voo cancelado, que nao chegou a partir.",
    "partida_real_data":         "Data da partida efetiva, hora de Brasilia.",
    "partida_real_hora":         "Hora e minuto da partida efetiva (HH:mm), hora de Brasilia.",
    "chegada_prevista_brasilia": "Horario de chegada programado, em hora de Brasilia. NAO e a hora local do aeroporto de destino.",
    "chegada_prevista_data":     "Data da chegada programada, hora de Brasilia.",
    "chegada_prevista_hora":     "Hora e minuto da chegada programada (HH:mm), hora de Brasilia.",
    "chegada_real_brasilia":     "Horario em que a aeronave efetivamente pousou, hora de Brasilia. Nulo em voo cancelado.",
    "chegada_real_data":         "Data da chegada efetiva, hora de Brasilia.",
    "chegada_real_hora":         "Hora e minuto da chegada efetiva (HH:mm), hora de Brasilia.",
    "situacao_voo":              "Situacao informada pela companhia: REALIZADO quando a etapa aconteceu, CANCELADO quando nao. Atencao: 23,4 por cento dos CANCELADO pertencem a voos-fantasma (par companhia+voo com 30 ou mais ocorrencias e 98 por cento ou mais canceladas). A gold marca isso.",
    "codigo_justificativa":      "Motivo declarado do atraso. Deixou de ser exigido pela ANAC em 2020 com a revogacao da IAC 1504: vem VAZIO em 100 por cento das linhas desta janela. Coluna morta, mantida por fidelidade a fonte.",
    "atraso_partida_min":        "Minutos entre a partida programada e a efetiva. Positivo e atraso, negativo e antecipacao. Aritmetica pura: NAO aplica limiar de pontualidade.",
    "atraso_chegada_min":        "Minutos entre a chegada programada e a efetiva. Positivo e atraso, negativo e antecipacao.",
    "minutos_recuperados":       "Atraso de partida menos atraso de chegada. Positivo significa que a etapa chegou MENOS ATRASADA do que saiu - e NAO que chegou no horario. Ver gold.obt_voos.resolveu_atraso.",
    "duracao_programada_min":    "Minutos entre partida e chegada programadas. Como os dois horarios estao no mesmo relogio, este valor E o tempo de bloco real, sem distorcao de fuso.",
    "duracao_real_min":          "Minutos entre partida e chegada efetivas. Combinado com a distancia da rota, permite checar plausibilidade fisica do horario na gold.",
    "_marcador_registro":        "Verdadeiro quando o horario previsto veio com fracao de segundo no arquivo. Assinatura do sistema de malha internacional: 90 por cento dessas linhas sao voos internacionais. Linhagem, nao qualidade.",
    "_arquivo_origem":           "Auditoria: nome do arquivo CSV mensal da ANAC de onde a linha veio.",
    "_fonte_atualizada_em":      "Auditoria: data em que a ANAC declarou ter atualizado o arquivo de origem.",
    "_ingerido_em":              "Auditoria: momento em que a linha entrou no bronze.",
    "_transformado_em":          "Auditoria: momento em que a silver foi reconstruida a partir do bronze.",
}

for coluna, comentario in COMENTARIOS_VRA.items():
    spark.sql(f"ALTER TABLE {CATALOGO}.silver.vra ALTER COLUMN {coluna} COMMENT '{sql_literal(comentario)}'")
print(f"{len(COMENTARIOS_VRA)} colunas comentadas em silver.vra")

# COMMAND ----------

COMENTARIOS_EMPRESAS = {
    "icao":                 "Codigo ICAO de tres letras. Vazio para operadores sem codigo (aviacao agricola, taxi aereo, aeroclube): so 167 dos 877 registros tem codigo.",
    "sigla_iata":           "Sigla de duas letras no padrao IATA. O cabecalho do arquivo original chama esta coluna de 'Estrangeira', o que nao descreve o conteudo.",
    "razao_social":         "Razao social da empresa aerea. E o nome que aparece para quem consome o produto final.",
    "servico":              "Tipo de servico autorizado pela ANAC: transporte regular, nao regular, aeroagricola, taxi aereo.",
    "cidade":               "Municipio da sede ou do representante legal no Brasil.",
    "uf":                   "Sigla da unidade federativa da sede.",
    "situacao":             "Situacao do registro na ANAC. Registro inativo permanece na tabela porque a empresa pode ter voado no periodo analisado.",
    "origem_cadastro":      "De qual dos dois cadastros da ANAC este registro veio: nacional ou estrangeira. E a coluna que preserva a fronteira entre as duas fontes depois da uniao.",
    "_arquivo_origem":      "Auditoria: arquivo CSV de origem.",
    "_fonte_atualizada_em": "Auditoria: data de publicacao declarada pela ANAC. Os dois cadastros tem datas DIFERENTES - conferir antes de comparar.",
    "_ingerido_em":         "Auditoria: momento da ingestao no bronze.",
    "_transformado_em":     "Auditoria: momento da construcao da silver.",
}

COMENTARIOS_AERODROMOS = {
    "icao":                 "Codigo ICAO (OACI) do aerodromo. Chave de ligacao com origem e destino do VRA.",
    "ciad":                 "Codigo de identificacao do aerodromo no cadastro da ANAC.",
    "nome":                 "Nome do aerodromo como publicado pela ANAC.",
    "municipio":            "Municipio onde o aerodromo esta fisicamente localizado.",
    "uf_nome":              "Nome da unidade federativa POR EXTENSO (Acre, Sao Paulo), nao a sigla: e assim que a ANAC publica. WHERE uf_nome = 'SP' devolve zero linhas.",
    "municipio_servido":    "Municipio principal atendido pelo aerodromo, que pode ser diferente do municipio onde ele fica. E a coluna certa para analisar praca.",
    "uf_servido_nome":      "Nome por extenso da UF do municipio servido.",
    "latitude":             "Latitude em GRAU DECIMAL, pronta para calculo de distancia. Vem da coluna LATGEOPOINT do arquivo.",
    "longitude":            "Longitude em GRAU DECIMAL, pronta para calculo de distancia. Vem da coluna LONGEOPOINT do arquivo.",
    "latitude_dms":         "Latitude em graus, minutos e segundos, como publicada pela ANAC. Formato de leitura, nao de calculo.",
    "longitude_dms":        "Longitude em graus, minutos e segundos. Formato de leitura, nao de calculo.",
    "altitude_m":           "Altitude do aerodromo em metros. Na origem vem com virgula decimal.",
    "operacao_diurna":      "Capacidade de operacao diurna publicada pela ANAC (VFR, IFR e categoria).",
    "operacao_noturna":     "Capacidade de operacao noturna. O valor 'Sem Operação' (130 aerodromos no cadastro) significa que o aerodromo NAO opera a noite.",
    "situacao_cadastro":    "Situacao do aerodromo no cadastro da ANAC.",
    "validade_registro":    "Validade do registro do aerodromo.",
    "_arquivo_origem":      "Auditoria: arquivo CSV de origem.",
    "_fonte_atualizada_em": "Auditoria: data de publicacao declarada pela ANAC.",
    "_ingerido_em":         "Auditoria: momento da ingestao no bronze.",
    "_transformado_em":     "Auditoria: momento da construcao da silver.",
}

COMENTARIOS_CODIGOS = {
    "dominio":          "A qual coluna do VRA este codigo pertence: codigo_di ou codigo_tipo_linha.",
    "codigo":           "O codigo como aparece no VRA.",
    "descricao":        "Descricao oficial do codigo, curada da pagina de descricao de variaveis da ANAC.",
    "catalogado":       "Falso quando o codigo aparece no dado mas NAO consta na tabela oficial da ANAC. Hoje so codigo_di = 1, com 6.338 voos.",
    "_fonte_url":       "Endereco da pagina da ANAC de onde a descricao foi curada.",
    "_transformado_em": "Auditoria: momento da construcao da silver.",
}

for tabela, mapa in [
    (f"{CATALOGO}.silver.empresas",         COMENTARIOS_EMPRESAS),
    (f"{CATALOGO}.silver.aerodromos",       COMENTARIOS_AERODROMOS),
    (f"{CATALOGO}.silver.codigos_operacao", COMENTARIOS_CODIGOS),
]:
    for coluna, comentario in mapa.items():
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{sql_literal(comentario)}'")
    print(f"{len(mapa)} colunas comentadas em {tabela}")

# COMMAND ----------

# MAGIC %md
# MAGIC Comentário de tabela e **tags**. Tag é metadado de busca e de política: é como alguém que nunca
# MAGIC viu este projeto encontra "todas as tabelas da camada silver" ou "tudo que é do domínio aviação"
# MAGIC sem precisar perguntar para a gente.

# COMMAND ----------

TABELAS = {
    f"{CATALOGO}.silver.vra": (
        "Silver - espelho governado de bronze.vra. Mesmo grao (uma linha por etapa de voo) e MESMA "
        "contagem de linhas do bronze: sem filtro, sem agregacao e sem regra de negocio. Traz tipagem, "
        "data e hora separadas, as metricas de aritmetica pura de atraso e duracao, e a chave tecnica "
        "_id_etapa. ATENCAO: os horarios estao em HORA DE BRASILIA (UTC-3), nao na hora local do "
        "aeroporto. Pontualidade, escopo, exclusoes e hora local ficam na gold.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "ANAC-VRA", "grao": "etapa_de_voo", "fuso": "america_sao_paulo"},
    ),
    f"{CATALOGO}.silver.empresas": (
        "Silver - cadastro unificado de empresas aereas: uniao dos dois cadastros do bronze (nacionais e "
        "estrangeiras) com a coluna origem_cadastro preservando a fonte de cada registro. Contagem igual a "
        "soma exata das duas tabelas de origem. Chave ICAO validada como unica por teste de contrato.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "ANAC-Operador-Aereo", "grao": "empresa"},
    ),
    f"{CATALOGO}.silver.aerodromos": (
        "Silver - espelho governado do cadastro de aerodromos publicos da ANAC. Traz latitude e longitude "
        "em grau decimal (calculo de distancia) e capacidade de operacao noturna. Cobre apenas aerodromos "
        "publicos brasileiros: aeroportos estrangeiros do VRA nao constam aqui, e isso e propriedade da "
        "fonte, nao defeito. 16 aeroportos brasileiros do VRA estao no cadastro de privados, fora do escopo.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "ANAC-Aerodromos", "grao": "aerodromo"},
    ),
    f"{CATALOGO}.silver.codigos_operacao": (
        "Silver - espelho da seed table de codigos de operacao (DI e tipo de linha) com as descricoes "
        "oficiais da ANAC e a marcacao de codigo nao catalogado.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "curadoria", "grao": "codigo"},
    ),
    f"{CATALOGO}.silver.duplicatas_origem": (
        "Silver - auditoria: grupos de linhas rigorosamente identicas publicados pela propria ANAC. "
        "42 grupos, 84 linhas, sempre em pares. 41 sao identicos em todas as colunas de negocio. "
        "Nenhuma linha foi removida da silver, que e espelho: esta tabela e evidencia, e a decisao de "
        "contar ou nao a repeticao como voo esta na gold (duplicata_de_origem).",
        {"camada": "silver", "dominio": "aviacao", "consumo": "qualidade", "grao": "grupo_duplicado"},
    ),
}

for tabela, (comentario, tags) in TABELAS.items():
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{sql_literal(comentario)}'")
    pares = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {tabela} SET TAGS ({pares})")
    print(f"{tabela}: comentario + {len(tags)} tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Auditoria da governança: 100% das colunas comentadas?
# MAGIC
# MAGIC "Documentei tudo" é afirmação, não fato. O `information_schema` responde de verdade:

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name,
           COUNT(*)                                                         AS colunas,
           SUM(CASE WHEN comment IS NULL OR comment = '' THEN 1 ELSE 0 END) AS sem_comentario,
           ROUND(100.0 * SUM(CASE WHEN comment IS NOT NULL AND comment <> '' THEN 1 ELSE 0 END)
                 / COUNT(*), 1)                                             AS pct_documentado
    FROM {CATALOGO}.information_schema.columns
    WHERE table_schema = 'silver'
    GROUP BY table_name ORDER BY table_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Fechamento do marco
# MAGIC
# MAGIC Cinco tabelas (quatro espelhos e uma de auditoria), todas documentadas, a `vra` com exatamente a
# MAGIC mesma contagem da origem, e os contratos verdes — com dois achados registrados: 137 colisões de
# MAGIC chave de negócio e 42 pares de linhas que a ANAC publica duplicadas.
# MAGIC
# MAGIC O que **não** está aqui, de propósito: `partida_pontual`, `escopo`, `voo_fantasma`, hora local,
# MAGIC qualquer agregação. O limiar de 15 minutos é uma decisão de negócio — outro uso pode pedir
# MAGIC 30. Se ele estivesse cravado na silver, atender esse outro cliente significaria reprocessar a
# MAGIC camada inteira. Na gold, é uma linha de SQL.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOGO}.silver"))
