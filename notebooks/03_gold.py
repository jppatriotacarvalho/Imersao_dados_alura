# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — regras de negócio, governança e saída
# MAGIC
# MAGIC A silver parou exatamente onde a decisão começa. Aqui ela começa.
# MAGIC
# MAGIC **Seis regras.** Nenhuma é opinião: cada uma nasceu de um número medido no dado, e cada uma muda
# MAGIC um KPI do projeto. A justificativa está escrita junto com o SQL, porque regra de negócio sem
# MAGIC justificativa é número mágico.
# MAGIC
# MAGIC | # | regra | o que decide | evidência |
# MAGIC |---|---|---|---|
# MAGIC | **R1** | `populacao_kpi` | quem entra no denominador | cancelamento é 0,00% em todo DI ≠ 0 e 2 |
# MAGIC | **R2** | `voo_fantasma` | o que é cancelamento de verdade | 0,67% da base = 23,4% dos cancelamentos |
# MAGIC | **R3** | `hora_local_origem` | a que horas o voo *realmente* sai | 42.235 etapas com hora deslocada |
# MAGIC | **R4** | `suspeita_erro_horario` | qual horário é implausível | 7.532 voos fora de 200–950 km/h |
# MAGIC | **R5** | `dia_atipico` | o que é desempenho da companhia e o que é evento | 10/12/2025: 17,85% vs mediana 2,72% |
# MAGIC | **R6** | `indice_confiabilidade_rota` | o que é uma rota confiável | a média esconde o p90 |
# MAGIC
# MAGIC **Roteiro deste notebook:**
# MAGIC dimensões → R2 → R5 → fato (R1, R3, R4) → R6 → agregados → OBT → governança → export para o MySQL.

# COMMAND ----------

from pyspark.sql import functions as F
import json

CATALOGO = "voebem"
EXPORT   = f"/Volumes/{CATALOGO}/gold/export"

# ---- decisoes de negocio, visiveis no topo e nao enterradas num CASE WHEN ----
LIMIAR_PONTUALIDADE_MIN  = 15      # criterio de pontualidade do projeto
FANTASMA_MIN_OCORRENCIAS = 30      # R2
FANTASMA_TAXA_MINIMA     = 0.98    # R2
VEL_MIN_KMH, VEL_MAX_KMH = 200, 950  # R4
ATIPICO_FATOR            = 3.0     # R5

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.gold")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.gold.export")

# COMMAND ----------

# MAGIC %md
# MAGIC ## R3 (parte 1) — a dimensão de aeroporto ganha fuso horário
# MAGIC
# MAGIC Os horários do VRA estão todos em hora de Brasília. Para saber a que horas o voo sai **no relógio
# MAGIC de quem está no aeroporto**, a dimensão precisa saber onde esse aeroporto fica no mapa de fusos.
# MAGIC Isso é decisão de modelagem — por isso está aqui e não na silver.
# MAGIC
# MAGIC O Brasil tem quatro fusos e não tem horário de verão desde 2019, então o offset é fixo e exato:
# MAGIC
# MAGIC | offset | onde |
# MAGIC |---|---|
# MAGIC | UTC−2 | Fernando de Noronha (`SBFN`) — cadastrado como Pernambuco, precisa de exceção |
# MAGIC | UTC−3 | Brasília e todo o litoral/centro-sul |
# MAGIC | UTC−4 | AM, RO, RR, MT, MS |
# MAGIC | UTC−5 | AC e o oeste do Amazonas (`SBTT` Tabatinga, `SWEI` Eirunepé) |
# MAGIC
# MAGIC **Aeroporto estrangeiro fica com `utc_offset` NULL de propósito.** Eles têm horário de verão, que
# MAGIC muda duas vezes por ano — um offset fixo seria uma aproximação disfarçada de fato. Melhor um NULL
# MAGIC honesto e documentado.
# MAGIC
# MAGIC ### O tamanho real do problema (e o que ele **não** é)
# MAGIC
# MAGIC Medido: **79 dos 496 aeródromos** do cadastro estão fora do UTC−3 (entre os 396 aeroportos do VRA,
# MAGIC são 34), e deles partem **42.235 etapas — 4,8%** das que têm fuso conhecido (4,2% dos 1.014.705
# MAGIC registros). Só essas mudam de hora na conversão.
# MAGIC
# MAGIC | aeroporto | pico pela hora de Brasília | pico pela hora local |
# MAGIC |---|---|---|
# MAGIC | SBRB Rio Branco (−5) | 02h | **00h** |
# MAGIC | SBEG Manaus (−4) | 12h | **11h** |
# MAGIC | SBCY Cuiabá (−4) | 03h | **02h** |
# MAGIC | SBGR Guarulhos (−3) | 08h | 08h |
# MAGIC
# MAGIC Vale dizer o que a regra **não** explica: Rio Branco tem 53% das partidas entre 00h e 04h no
# MAGIC relógio de Brasília e **ainda tem 42% depois de converter**. A madrugada de Rio Branco é real — é
# MAGIC assim que a malha amazônica funciona. O fuso desloca o horário em 2 horas, não inventa o fenômeno.
# MAGIC Vender a regra como "a madrugada de RBR é ilusão de fuso" seria exagero, e exagero numa
# MAGIC apresentação é o que a banca encontra primeiro.
# MAGIC
# MAGIC O que a regra entrega, então:
# MAGIC
# MAGIC 1. **A descrição da coluna passa a bater com o dado.** A descrição anterior dizia "hora local do
# MAGIC    aeroporto de origem", mas 100% das linhas estão em hora de Brasília. Quando o consumidor é um LLM,
# MAGIC    descrição errada não é imprecisão — é defeito funcional.
# MAGIC 2. **Análise por aeroporto do Norte e Centro-Oeste fica correta.** Pico e faixa horária dos
# MAGIC    aeroportos fora do UTC−3 (34 no VRA) estavam 1h ou 2h deslocados.
# MAGIC 3. **As duas perguntas ficam separadas.** `hora_brasilia` para propagação na rede (todo mundo no
# MAGIC    mesmo relógio, que é o que a cascata exige); `hora_local_origem` para o pico de um aeroporto.

# COMMAND ----------

# excecoes que a UF nao resolve (oeste do Amazonas e Fernando de Noronha)
OVERRIDE_FUSO = {"SBFN": -2, "SBTT": -5, "SWEI": -5}
override_sql = " ".join(f"WHEN a.icao = '{k}' THEN {v}" for k, v in OVERRIDE_FUSO.items())

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.dim_aeroporto AS
WITH usados AS (
  SELECT icao_origem  AS icao FROM {CATALOGO}.silver.vra
  UNION
  SELECT icao_destino AS icao FROM {CATALOGO}.silver.vra
)
SELECT
  u.icao                                                   AS icao_aeroporto,

  COALESCE(a.nome, concat('AEROPORTO FORA DO CADASTRO ANAC (', u.icao, ')')) AS nome_aeroporto,
  a.municipio                                              AS municipio_aeroporto,
  COALESCE(a.municipio_servido, a.municipio)               AS praca_aeroporto,

  -- O cadastro da ANAC tem 4 aerodromos com Municipio e UF VAZIOS, mas com
  -- Municipio Servido e UF Servido preenchidos: SBIZ (Imperatriz/MA, 2.912
  -- pontas de voo), SBCR (Corumba/MS, 218), SBME e SDRS (sem voo no periodo).
  -- Sem este COALESCE eles ficam sem UF e, pior, caem no ELSE NULL do fuso
  -- abaixo e perdem a hora local. Foi um teste de carga no MySQL que pegou:
  -- 236 aeroportos sem fuso, mas so 234 fora do cadastro.
  COALESCE(a.uf_nome, a.uf_servido_nome)                   AS uf_aeroporto,
  a.latitude,
  a.longitude,
  a.altitude_m,
  a.operacao_noturna,

  -- regra de negocio: opera a noite ou nao. 'Sem Operação' e o unico valor que significa NAO.
  CASE WHEN a.operacao_noturna IS NULL THEN NULL
       WHEN a.operacao_noturna = 'Sem Operação' THEN FALSE
       ELSE TRUE END                                       AS opera_a_noite,

  -- pais deduzido do prefixo ICAO: SB/SD/SI/SJ/SN/SS/SW sao brasileiros
  CASE WHEN left(u.icao, 1) = 'S' AND substr(u.icao, 2, 1) IN ('B','D','I','J','N','S','W')
       THEN 'Brasil' ELSE 'Exterior' END                   AS pais_aeroporto,

  -- ---------- R3: fuso horario ----------
  CASE
    {override_sql}
    WHEN COALESCE(a.uf_nome, a.uf_servido_nome) = 'Acre'                                   THEN -5
    WHEN COALESCE(a.uf_nome, a.uf_servido_nome)
         IN ('Amazonas','Rondônia','Roraima','Mato Grosso','Mato Grosso do Sul')           THEN -4
    WHEN COALESCE(a.uf_nome, a.uf_servido_nome) IS NOT NULL                                THEN -3
    ELSE NULL   -- estrangeiro: NULL honesto, nao aproximacao
  END                                                      AS utc_offset,

  (a.icao IS NOT NULL)                                     AS no_cadastro_anac,
  current_timestamp()                                      AS _processado_em
FROM usados u
LEFT JOIN {CATALOGO}.silver.aerodromos a ON a.icao = u.icao
""")

display(spark.sql(f"""
    SELECT pais_aeroporto, no_cadastro_anac, utc_offset,
           COUNT(*) AS aeroportos,
           SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) AS com_coordenada
    FROM {CATALOGO}.gold.dim_aeroporto
    GROUP BY 1,2,3 ORDER BY 1,2,3
"""))

# CONTRATO: aerodromo que ESTA no cadastro da ANAC nao pode ficar sem fuso.
# Quem cai aqui e aerodromo com UF vazia - e o COALESCE com uf_servido_nome
# acima e o que evita isso.
sem_fuso = spark.sql(f"""
    SELECT icao_aeroporto, nome_aeroporto, praca_aeroporto
    FROM {CATALOGO}.gold.dim_aeroporto
    WHERE no_cadastro_anac AND utc_offset IS NULL
""")
n = sem_fuso.count()
print(f"[{'OK ' if n == 0 else 'FALHA'}] aerodromo cadastrado sem fuso: {n} (esperado 0)")
if n:
    display(sem_fuso)
    raise AssertionError("Aerodromo no cadastro ficou sem utc_offset - conferir uf_nome/uf_servido_nome")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão de companhia
# MAGIC
# MAGIC Construída a partir dos códigos que **aparecem no fato**, não do cadastro inteiro — o cadastro tem
# MAGIC 877 registros, dos quais 710 não têm código ICAO (aviação agrícola, táxi aéreo, aeroclube). Uma
# MAGIC dimensão feita do cadastro seria 85% vazia.
# MAGIC
# MAGIC Fallback textual em vez de `NULL`: medido, só 7 códigos do VRA não têm cadastro (69 voos no total),
# MAGIC mas `NULL` num gráfico vira barra em branco, e barra em branco vira pergunta.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.dim_empresa AS
WITH usadas AS (SELECT DISTINCT icao_empresa FROM {CATALOGO}.silver.vra)
SELECT
  u.icao_empresa,
  COALESCE(e.razao_social, concat('COMPANHIA NAO CADASTRADA (', u.icao_empresa, ')')) AS nome_companhia,
  e.sigla_iata,
  e.servico                              AS servico_autorizado,
  e.origem_cadastro                      AS cadastro_companhia,
  (e.icao IS NOT NULL)                   AS no_cadastro_anac,
  current_timestamp()                    AS _processado_em
FROM usadas u
LEFT JOIN (SELECT * FROM {CATALOGO}.silver.empresas WHERE icao IS NOT NULL AND icao <> '') e
       ON e.icao = u.icao_empresa
""")

print(f"dim_empresa: {spark.table(f'{CATALOGO}.gold.dim_empresa').count()} companhias")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## R2 — `voo_fantasma`: 23,4% dos cancelamentos vêm de 54 números de voo quase sempre cancelados
# MAGIC
# MAGIC A primeira versão desta regra estava errada, e vale contar por quê.
# MAGIC
# MAGIC O caminho óbvio era usar o `_marcador_registro` (a fração de segundo que marca o sistema de malha
# MAGIC internacional): registros com marcador cancelam 17,7%, sem marcador cancelam 1,6%. Parecia regra.
# MAGIC **É regra ruim** — a Turkish tem 98,8% dos registros com marcador e cancela 1,44%. A regra teria
# MAGIC punido a Turkish sem motivo.
# MAGIC
# MAGIC A resposta apareceu quando se olha **voo a voo**, não companhia a companhia:
# MAGIC
# MAGIC ```
# MAGIC IBE 0268 → 366 ocorrências,  11 cancelados   (3,0%)   ← operado quase sempre
# MAGIC IBE 0102 → 363 ocorrências, 363 cancelados (100,0%)   ← nenhuma partida registrada
# MAGIC IBE 0106 → 361 ocorrências, 361 cancelados (100,0%)
# MAGIC ETH 3739 → 314 ocorrências, 314 cancelados (100,0%)
# MAGIC ```
# MAGIC
# MAGIC Um número de voo cancelado nas 363 vezes em que aparece, durante 12 meses, não se comporta como um
# MAGIC voo cancelado de vez em quando. **O dado não diz por que esses registros existem**; a regra só os
# MAGIC separa, para que não pesem na taxa de cancelamento.
# MAGIC
# MAGIC **Regra:** par `(empresa, número do voo)` com ≥ 30 ocorrências e ≥ 98% de cancelamento.
# MAGIC
# MAGIC - 54 pares, **6.813 linhas — 0,67% da base**
# MAGIC - **23,4% de todos os cancelamentos** do período
# MAGIC - apenas **2** dessas 6.813 linhas têm algum `partida_real` (as outras 6.811 estão canceladas)
# MAGIC
# MAGIC Efeito no ranking: Iberia sai de 54,61% para **6,46%**. Arajet de 73,79% para 24,83%. Taxa nacional
# MAGIC sobre todos os registros (a mesma conta do ranking): de 2,87% para 2,22%. Sobre os voos elegíveis da
# MAGIC R1 (`entra_em_cancelamento`, com `cancelamento_operacional`) ela fica em 2,27%. GOL e LATAM não se mexem.
# MAGIC
# MAGIC Os limiares (30 e 98%) são decisão, e decisão fica no topo do notebook. Com 30 ocorrências em 12
# MAGIC meses, um voo sazonal legítimo não é pego por acidente; com 98% em vez de 100%, um registro
# MAGIC corrigido no meio do caminho não escapa.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.voos_fantasma AS
SELECT
  icao_empresa,
  numero_voo,
  COUNT(*)                                                           AS ocorrencias,
  SUM(CASE WHEN situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END)        AS cancelados,
  ROUND(100.0 * SUM(CASE WHEN situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_cancelamento,
  SUM(CASE WHEN partida_real_brasilia IS NOT NULL THEN 1 ELSE 0 END) AS com_partida_real,
  current_timestamp()                                                AS _processado_em
FROM {CATALOGO}.silver.vra
GROUP BY icao_empresa, numero_voo
HAVING COUNT(*) >= {FANTASMA_MIN_OCORRENCIAS}
   AND SUM(CASE WHEN situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END) / COUNT(*) >= {FANTASMA_TAXA_MINIMA}
""")

display(spark.sql(f"""
    SELECT f.icao_empresa, e.nome_companhia, f.numero_voo,
           f.ocorrencias, f.cancelados, f.taxa_cancelamento, f.com_partida_real
    FROM {CATALOGO}.gold.voos_fantasma f
    LEFT JOIN {CATALOGO}.gold.dim_empresa e USING (icao_empresa)
    ORDER BY ocorrencias DESC LIMIT 20
"""))

display(spark.sql(f"""
    SELECT COUNT(*)              AS pares_fantasma,
           SUM(ocorrencias)      AS linhas_afetadas,
           SUM(cancelados)       AS cancelamentos_removidos,
           SUM(com_partida_real) AS que_chegaram_a_partir,
           ROUND(100.0 * SUM(cancelados) /
                 (SELECT COUNT(*) FROM {CATALOGO}.silver.vra WHERE situacao_voo = 'CANCELADO'), 1)
                 AS pct_dos_cancelamentos_do_pais
    FROM {CATALOGO}.gold.voos_fantasma
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## R5 — `dia_atipico`: separar o evento do desempenho
# MAGIC
# MAGIC Em 10/12/2025, **17,85%** dos voos foram cancelados. No dia seguinte, 14,19%. A mediana diária do
# MAGIC período é **2,72%**.
# MAGIC
# MAGIC Sem essa flag, dezembro pune a companhia por um evento que não é dela. Com ela, os dias atípicos
# MAGIC ficam marcados: o painel mostra a taxa diária com a mediana e o corte da R5, e os agregados de
# MAGIC horário (`kpi_aeroporto_hora`) já saem sem esses dias.
# MAGIC
# MAGIC Baseline = **mediana** do período, não média: a média é contaminada pelos próprios dias atípicos
# MAGIC que a regra quer detectar. Corte em 3× a mediana (≈ 8,2%), o que marca 3 dias em 365.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.dias_atipicos AS
WITH diario AS (
  SELECT partida_prevista_data AS dia,
         COUNT(*) AS voos,
         SUM(CASE WHEN situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelados
  FROM {CATALOGO}.silver.vra
  WHERE partida_prevista_data IS NOT NULL
  GROUP BY 1
  HAVING COUNT(*) >= 500          -- ignora as bordas do periodo, com poucos voos
),
com_taxa AS (SELECT *, cancelados / voos AS taxa FROM diario),
base     AS (SELECT percentile_approx(taxa, 0.5) AS mediana FROM com_taxa)
SELECT c.dia, c.voos, c.cancelados,
       ROUND(100 * c.taxa, 2)                 AS taxa_pct,
       ROUND(100 * b.mediana, 2)              AS mediana_periodo_pct,
       ROUND(c.taxa / b.mediana, 2)           AS vezes_a_mediana,
       (c.taxa > {ATIPICO_FATOR} * b.mediana) AS dia_atipico,
       current_timestamp()                    AS _processado_em
FROM com_taxa c CROSS JOIN base b
""")

display(spark.sql(f"""
    SELECT * EXCEPT (_processado_em) FROM {CATALOGO}.gold.dias_atipicos
    ORDER BY taxa_pct DESC LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## O fato — onde R1, R3 e R4 acontecem
# MAGIC
# MAGIC ### R1 — `populacao_kpi`: o denominador honesto
# MAGIC
# MAGIC Medido, e é o achado mais bonito do projeto:
# MAGIC
# MAGIC | DI | linhas | % sem horário previsto | % cancelado |
# MAGIC |---|---|---|---|
# MAGIC | **0** Regular | 974.802 | **0,0%** | **2,91%** |
# MAGIC | 1 *(não catalogado)* | 6.338 | 97,6% | **0,00%** |
# MAGIC | **2** Extra | 8.334 | 83,4% | **9,38%** |
# MAGIC | 3 Retorno | 3.919 | 100,0% | **0,00%** |
# MAGIC | 4 Inclusão de Etapa | 6.407 | 99,9% | **0,00%** |
# MAGIC | D **Duplicada** | 694 | 100,0% | **0,00%** |
# MAGIC
# MAGIC Duas leituras, e nenhuma é estatística — as duas são definição:
# MAGIC
# MAGIC 1. **`DI = 0` é o único que sempre tem horário previsto** (zero exceções em 974.802 linhas). Não
# MAGIC    faltou o dado: o voo não estava programado. Logo, só `DI = 0` pode ser *pontual*.
# MAGIC 2. **Cancelamento é 0,00% em todo DI exceto 0 e 2.** Voo não programado não pode ser cancelado —
# MAGIC    ele só existe no dado porque aconteceu. Logo, só DI 0 ou 2 pode ser *cancelado*.
# MAGIC
# MAGIC E `DI = 'D'` é "Etapa de Voo Duplicada": a própria ANAC avisa que aquilo é repetição. São 694 linhas,
# MAGIC todas em cargueiras (Cargolux, Lufthansa Cargo, Tampa). Contar como voo é contar duas vezes.
# MAGIC
# MAGIC **E tem uma terceira exclusão, que o contrato da silver descobriu:** `duplicata_de_origem`.
# MAGIC A ANAC publica **42 pares de linhas repetidas** — mesma companhia, mesmo voo, mesma rota, mesmo
# MAGIC horário previsto, mesmo horário real (41 dos 42 pares são idênticos em todas as colunas de negócio).
# MAGIC Não é `DI = 'D'` (a fonte não marca essas), não é colisão de hash: é a linha repetida no arquivo.
# MAGIC
# MAGIC ```
# MAGIC QTR 8172  SBGR→SKBO  07/12 15:15  real 15:07  chegou 20:42  REALIZADO
# MAGIC QTR 8172  SBGR→SKBO  07/12 15:15  real 15:07  chegou 20:42  REALIZADO   <- idêntica
# MAGIC ```
# MAGIC
# MAGIC São 42 linhas excedentes — pouco em volume, mas é contagem de voo errada, e é o tipo de coisa que
# MAGIC ninguém encontra depois. A silver marcou com `_ocorrencia`; aqui a segunda ocorrência sai do
# MAGIC denominador.
# MAGIC
# MAGIC Em todos os três casos a linha **fica na tabela** — a gold não apaga histórico. Ela sai do
# MAGIC **denominador**.
# MAGIC
# MAGIC ### R4 — `suspeita_erro_horario`: física como controle de qualidade
# MAGIC
# MAGIC Uma faixa de plausibilidade de atraso ("atraso entre −2h e +24h") não pega a data errada num salto
# MAGIC curto: a duração programada fica longa demais para a distância, e a velocidade despenca.
# MAGIC
# MAGIC Com latitude e longitude em grau decimal, dá para calcular a distância de grande círculo em **80,8%**
# MAGIC das etapas e derivar a velocidade média programada: mediana **498 km/h**, p95 687 km/h. Fora de
# MAGIC 200–950 km/h, **0,94% dos voos (7.532)** — suspeita de erro de horário que um limiar de atraso não pega.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.fato_voos AS
WITH base AS (
  SELECT
    v.*,
    o.latitude AS lat_o, o.longitude AS lon_o, o.utc_offset AS utc_o, o.pais_aeroporto AS pais_o,
    d.latitude AS lat_d, d.longitude AS lon_d, d.utc_offset AS utc_d, d.opera_a_noite  AS destino_opera_a_noite,
    (f.numero_voo IS NOT NULL)     AS voo_fantasma,
    COALESCE(t.dia_atipico, FALSE) AS dia_atipico
  FROM {CATALOGO}.silver.vra v
  LEFT JOIN {CATALOGO}.gold.dim_aeroporto o ON o.icao_aeroporto = v.icao_origem
  LEFT JOIN {CATALOGO}.gold.dim_aeroporto d ON d.icao_aeroporto = v.icao_destino
  LEFT JOIN {CATALOGO}.gold.voos_fantasma  f ON f.icao_empresa = v.icao_empresa
                                            AND f.numero_voo   = v.numero_voo
  LEFT JOIN {CATALOGO}.gold.dias_atipicos  t ON t.dia = v.partida_prevista_data
),
geo AS (
  SELECT *,
    -- ---------- R4: distancia de grande circulo (haversine), em km ----------
    CASE WHEN lat_o IS NOT NULL AND lat_d IS NOT NULL THEN
      2 * 6371 * asin(sqrt(
            pow(sin(radians(lat_d - lat_o) / 2), 2)
          + cos(radians(lat_o)) * cos(radians(lat_d)) * pow(sin(radians(lon_d - lon_o) / 2), 2)))
    END AS km_rota
  FROM base
)
SELECT
  _id_etapa,

  -- ---------- chaves ----------
  icao_empresa,
  numero_voo,
  icao_origem,
  icao_destino,
  concat(icao_origem, ' - ', icao_destino)                  AS rota_icao,

  -- ---------- operacao ----------
  codigo_di,
  codigo_tipo_linha,
  CASE WHEN codigo_tipo_linha IN ('N','C') THEN 'Domestico'
       WHEN codigo_tipo_linha IN ('I','G') THEN 'Internacional' END          AS escopo_voo,
  CASE WHEN codigo_tipo_linha IN ('C','G') THEN 'Cargueiro' ELSE 'Misto' END AS natureza_voo,

  -- ================= R1: POPULACAO =================
  (codigo_di = '0')                                          AS voo_programado,
  (codigo_di = 'D')                                          AS etapa_duplicada_anac,
  (_ocorrencia > 1)                                          AS duplicata_de_origem,
  (codigo_di = '0'        AND _ocorrencia = 1)               AS entra_em_pontualidade,
  (codigo_di IN ('0','2') AND _ocorrencia = 1)               AS entra_em_cancelamento,

  -- ================= R2: VOO-FANTASMA =================
  voo_fantasma,
  (situacao_voo = 'CANCELADO' AND NOT voo_fantasma)         AS cancelamento_operacional,

  -- ================= R5: DIA ATIPICO =================
  dia_atipico,

  -- ---------- tempo: hora de Brasilia (relogio do arquivo) ----------
  partida_prevista_brasilia,
  partida_prevista_data,
  hour(partida_prevista_brasilia)                           AS hora_brasilia,
  date_trunc('MONTH', partida_prevista_brasilia)            AS mes_referencia,

  -- dia da semana escrito a mao: date_format('EEEE') depende do locale do cluster
  -- e devolveria 'monday' num cluster en-US. Mapa explicito nao depende de ambiente.
  CASE dayofweek(partida_prevista_brasilia)
       WHEN 1 THEN 'domingo' WHEN 2 THEN 'segunda' WHEN 3 THEN 'terca'
       WHEN 4 THEN 'quarta'  WHEN 5 THEN 'quinta'  WHEN 6 THEN 'sexta'
       WHEN 7 THEN 'sabado'  END                            AS dia_semana,
  dayofweek(partida_prevista_brasilia)                      AS num_dia_semana,

  partida_real_brasilia,
  chegada_prevista_brasilia,
  chegada_real_brasilia,

  -- ================= R3: HORA LOCAL DO AEROPORTO =================
  timestampadd(HOUR, utc_o + 3, partida_prevista_brasilia)       AS partida_prevista_local,
  hour(timestampadd(HOUR, utc_o + 3, partida_prevista_brasilia)) AS hora_local_origem,
  timestampadd(HOUR, utc_d + 3, chegada_prevista_brasilia)       AS chegada_prevista_local,
  hour(timestampadd(HOUR, utc_d + 3, chegada_prevista_brasilia)) AS hora_local_destino,

  -- ---------- metricas de atraso ----------
  atraso_partida_min,
  atraso_chegada_min,
  minutos_recuperados,
  duracao_programada_min,
  duracao_real_min,

  -- ================= R4: PLAUSIBILIDADE FISICA =================
  ROUND(km_rota, 1)                                         AS km_rota,
  CASE WHEN km_rota IS NULL THEN NULL
       WHEN km_rota <  400  THEN 'curta'
       WHEN km_rota < 1000  THEN 'media'
       WHEN km_rota < 2000  THEN 'longa'
       ELSE 'muito_longa' END                               AS faixa_etapa,
  ROUND(km_rota / NULLIF(duracao_programada_min, 0) * 60, 1) AS velocidade_programada_kmh,
  CASE WHEN km_rota IS NULL OR duracao_programada_min IS NULL OR duracao_programada_min <= 0 THEN NULL
       ELSE (km_rota / duracao_programada_min * 60) NOT BETWEEN {VEL_MIN_KMH} AND {VEL_MAX_KMH}
  END                                                       AS suspeita_erro_horario,

  -- ---------- pontualidade (limiar do projeto) ----------
  CASE WHEN codigo_di <> '0' OR atraso_partida_min IS NULL THEN NULL
       ELSE atraso_partida_min <= {LIMIAR_PONTUALIDADE_MIN} END  AS partida_pontual,
  CASE WHEN codigo_di <> '0' OR atraso_chegada_min IS NULL THEN NULL
       ELSE atraso_chegada_min <= {LIMIAR_PONTUALIDADE_MIN} END  AS chegada_pontual,
  CASE WHEN atraso_chegada_min IS NULL                     THEN NULL
       WHEN atraso_chegada_min <= {LIMIAR_PONTUALIDADE_MIN} THEN 'no_horario'
       WHEN atraso_chegada_min <=  30                       THEN 'leve'
       WHEN atraso_chegada_min <=  60                       THEN 'moderado'
       WHEN atraso_chegada_min <= 120                       THEN 'severo'
       ELSE 'critico' END                                   AS classe_atraso,

  -- recuperou E resolveu: recuperar tempo no ar nao significa chegar no horario
  (minutos_recuperados > 0 AND atraso_chegada_min <= {LIMIAR_PONTUALIDADE_MIN}) AS resolveu_atraso,

  -- ---------- situacao ----------
  situacao_voo,
  (situacao_voo = 'REALIZADO')                              AS voo_realizado,
  (situacao_voo = 'CANCELADO')                              AS voo_cancelado,
  destino_opera_a_noite,

  _marcador_registro                                        AS registro_malha_internacional,
  _arquivo_origem,
  _fonte_atualizada_em,
  current_timestamp()                                       AS _processado_em
FROM geo
""")

print(f"gold.fato_voos: {spark.table(f'{CATALOGO}.gold.fato_voos').count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### O que as regras fizeram com os números do projeto

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'A) tudo' AS definicao, COUNT(*) AS voos,
           ROUND(100*AVG(CASE WHEN voo_cancelado THEN 1.0 ELSE 0 END), 2) AS cancelamento_pct
    FROM {CATALOGO}.gold.fato_voos
    UNION ALL
    SELECT 'B) R1: so DI 0 e 2', COUNT(*),
           ROUND(100*AVG(CASE WHEN voo_cancelado THEN 1.0 ELSE 0 END), 2)
    FROM {CATALOGO}.gold.fato_voos WHERE entra_em_cancelamento
    UNION ALL
    SELECT 'C) R1 + R2: sem voo-fantasma', COUNT(*),
           ROUND(100*AVG(CASE WHEN voo_cancelado THEN 1.0 ELSE 0 END), 2)
    FROM {CATALOGO}.gold.fato_voos WHERE entra_em_cancelamento AND NOT voo_fantasma
    UNION ALL
    SELECT 'D) C + so dia normal (R5)', COUNT(*),
           ROUND(100*AVG(CASE WHEN voo_cancelado THEN 1.0 ELSE 0 END), 2)
    FROM {CATALOGO}.gold.fato_voos WHERE entra_em_cancelamento AND NOT voo_fantasma AND NOT dia_atipico
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT e.nome_companhia,
           COUNT(*)                                                         AS registros,
           ROUND(100*AVG(CASE WHEN f.voo_cancelado THEN 1.0 ELSE 0 END), 2) AS cancelamento_bruto,
           SUM(CASE WHEN f.voo_fantasma THEN 1 ELSE 0 END)                  AS linhas_fantasma,
           ROUND(100*AVG(CASE WHEN f.voo_fantasma THEN NULL
                              WHEN f.voo_cancelado THEN 1.0 ELSE 0 END), 2) AS cancelamento_real
    FROM {CATALOGO}.gold.fato_voos f
    JOIN {CATALOGO}.gold.dim_empresa e USING (icao_empresa)
    GROUP BY 1 HAVING COUNT(*) >= 2000
    ORDER BY cancelamento_bruto DESC LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### R3 na prática: quantas linhas a regra realmente move
# MAGIC
# MAGIC A pergunta honesta não é "a regra é importante?", é "quantas linhas ela muda?".

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*)                                                              AS etapas_com_fuso_conhecido,
           SUM(CASE WHEN hora_brasilia <> hora_local_origem THEN 1 ELSE 0 END)   AS etapas_que_mudam_de_hora,
           ROUND(100.0 * SUM(CASE WHEN hora_brasilia <> hora_local_origem THEN 1 ELSE 0 END)
                 / COUNT(*), 1)                                                  AS pct
    FROM {CATALOGO}.gold.fato_voos
    WHERE hora_local_origem IS NOT NULL
"""))

# COMMAND ----------

display(spark.sql(f"""
    WITH picos AS (
      SELECT icao_origem, hora_brasilia, hora_local_origem, COUNT(*) AS voos,
             ROW_NUMBER() OVER (PARTITION BY icao_origem ORDER BY COUNT(*) DESC) AS rn
      FROM {CATALOGO}.gold.fato_voos
      WHERE hora_local_origem IS NOT NULL
      GROUP BY 1,2,3
    )
    SELECT a.icao_aeroporto, a.nome_aeroporto, a.utc_offset,
           p.hora_brasilia AS pico_hora_brasilia, p.hora_local_origem AS pico_hora_local, p.voos
    FROM picos p JOIN {CATALOGO}.gold.dim_aeroporto a ON a.icao_aeroporto = p.icao_origem
    WHERE p.rn = 1 AND p.icao_origem IN ('SBRB','SBEG','SBCY','SBPV','SBBV','SBCG','SBGR','SBFZ')
    ORDER BY a.utc_offset, a.icao_aeroporto
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## R6 — `indice_confiabilidade_rota`: olhar a cauda, não só a média
# MAGIC
# MAGIC O **atraso médio** sozinho não responde a pergunta de quem tem conexão.
# MAGIC
# MAGIC Uma rota que atrasa 12 minutos todo dia é gerenciável: você sai de casa 15 minutos antes e acabou.
# MAGIC Uma rota que sai no horário em 80% dos dias e atrasa 3 horas nos outros 20% é **inviável para quem
# MAGIC tem conexão** — e as duas podem ter a mesma média.
# MAGIC
# MAGIC **Premissa do índice: para quem tem conexão, a variabilidade pesa tanto quanto o valor central.** Por isso o índice pesa o p90 do
# MAGIC atraso, não a média:
# MAGIC
# MAGIC ```
# MAGIC 40% × pontualidade de chegada
# MAGIC 30% × (1 − taxa de cancelamento operacional)
# MAGIC 30% × (1 − p90 do atraso de chegada, normalizado em 120 min)
# MAGIC ```
# MAGIC
# MAGIC Os pesos são decisão, e estão à vista. O teto de 120 min no p90 também: acima de duas horas, o
# MAGIC índice trata todo atraso como igualmente ruim.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.kpi_rota_mensal AS
SELECT
  rota_icao,
  icao_origem,
  icao_destino,
  icao_empresa,
  mes_referencia,
  escopo_voo,
  MAX(faixa_etapa)                                          AS faixa_etapa,
  MAX(km_rota)                                              AS km_rota,

  COUNT(*)                                                  AS voos_previstos,
  SUM(CASE WHEN voo_realizado THEN 1 ELSE 0 END)            AS voos_realizados,
  SUM(CASE WHEN cancelamento_operacional THEN 1 ELSE 0 END) AS cancelamentos,

  ROUND(AVG(CASE WHEN chegada_pontual THEN 1.0 WHEN chegada_pontual IS NOT NULL THEN 0 END), 4) AS pontualidade_chegada,
  ROUND(AVG(CASE WHEN cancelamento_operacional THEN 1.0 ELSE 0 END), 4)                         AS taxa_cancelamento,
  ROUND(AVG(atraso_chegada_min), 1)                                                             AS atraso_medio,
  ROUND(percentile_approx(atraso_chegada_min, 0.5), 1)                                          AS atraso_p50,
  ROUND(percentile_approx(atraso_chegada_min, 0.9), 1)                                          AS atraso_p90,

  -- ================= R6 =================
  ROUND(100 * (
      0.40 * COALESCE(AVG(CASE WHEN chegada_pontual THEN 1.0 WHEN chegada_pontual IS NOT NULL THEN 0 END), 0)
    + 0.30 * (1 - AVG(CASE WHEN cancelamento_operacional THEN 1.0 ELSE 0 END))
    + 0.30 * (1 - LEAST(GREATEST(COALESCE(percentile_approx(atraso_chegada_min, 0.9), 0), 0) / 120.0, 1))
  ), 1)                                                     AS indice_confiabilidade,

  current_timestamp()                                       AS _processado_em
FROM {CATALOGO}.gold.fato_voos
WHERE entra_em_cancelamento
  AND NOT voo_fantasma
  AND NOT etapa_duplicada_anac
  AND NOT duplicata_de_origem
  AND COALESCE(suspeita_erro_horario, FALSE) = FALSE
  AND mes_referencia IS NOT NULL
GROUP BY rota_icao, icao_origem, icao_destino, icao_empresa, mes_referencia, escopo_voo
HAVING COUNT(*) >= 20
""")

display(spark.sql(f"""
    SELECT rota_icao, icao_empresa, SUM(voos_previstos) AS voos,
           ROUND(AVG(100*pontualidade_chegada), 1) AS pontualidade_pct,
           ROUND(AVG(atraso_medio), 1)             AS atraso_medio,
           ROUND(AVG(atraso_p90), 1)               AS atraso_p90,
           ROUND(AVG(indice_confiabilidade), 1)    AS indice
    FROM {CATALOGO}.gold.kpi_rota_mensal
    GROUP BY 1,2 HAVING SUM(voos_previstos) >= 2000
    ORDER BY indice DESC LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exemplo: por que o p90 importa
# MAGIC
# MAGIC Duas rotas medidas neste dataset, com atraso médio praticamente idêntico (atraso de chegada calculado
# MAGIC sobre o período inteiro, direto nas etapas):
# MAGIC
# MAGIC | rota | companhia | voos | atraso médio | **p90** |
# MAGIC |---|---|---|---|---|
# MAGIC | SBNF → SBGR | LATAM | 1.416 | 7,5 min | **40,0 min** |
# MAGIC | SBGR → SBGO | GOL | 1.264 | 7,1 min | **26,0 min** |
# MAGIC
# MAGIC Pela média, são quase iguais; pelo p90, não. A query abaixo lista todas as que estão nessa situação.
# MAGIC Ela usa a média dos p90 mensais da `kpi_rota_mensal` (a mesma conta do dashboard), que dá 7,6 / 41,4 min
# MAGIC e 7,2 / 26,7 min para essas duas rotas.

# COMMAND ----------

display(spark.sql(f"""
    WITH r AS (
      SELECT rota_icao, icao_empresa,
             SUM(voos_previstos) AS voos,
             ROUND(AVG(atraso_medio), 1)          AS atraso_medio,
             ROUND(AVG(atraso_p90), 1)            AS atraso_p90,
             ROUND(AVG(indice_confiabilidade), 1) AS indice
      FROM {CATALOGO}.gold.kpi_rota_mensal
      GROUP BY 1,2 HAVING SUM(voos_previstos) >= 1000
    )
    SELECT * FROM r WHERE atraso_medio BETWEEN 5 AND 12
    ORDER BY atraso_p90 DESC LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Agregados para o dashboard
# MAGIC
# MAGIC O dashboard **não vai ler 1 milhão de linhas**. Ele lê tabelas de poucos milhares, que cabem
# MAGIC embutidas como JSON dentro da própria página. A `obt_voos` fica reservada para o agente de IA.
# MAGIC
# MAGIC Repare que os agregados guardam **numerador e denominador separados**, não o percentual pronto:
# MAGIC percentual não se soma, e quem tentar somar vai errar.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.kpi_diario AS
SELECT
  partida_prevista_data                                        AS dia,
  icao_empresa,
  escopo_voo,
  dia_atipico,
  COUNT(*)                                                     AS voos,
  SUM(CASE WHEN voo_realizado THEN 1 ELSE 0 END)               AS realizados,
  SUM(CASE WHEN cancelamento_operacional THEN 1 ELSE 0 END)    AS cancelados,
  SUM(CASE WHEN voo_fantasma THEN 1 ELSE 0 END)                AS linhas_fantasma,
  SUM(CASE WHEN partida_pontual THEN 1 ELSE 0 END)             AS partidas_pontuais,
  SUM(CASE WHEN partida_pontual IS NOT NULL THEN 1 ELSE 0 END) AS partidas_avaliaveis,
  SUM(CASE WHEN chegada_pontual THEN 1 ELSE 0 END)             AS chegadas_pontuais,
  SUM(CASE WHEN chegada_pontual IS NOT NULL THEN 1 ELSE 0 END) AS chegadas_avaliaveis,
  ROUND(AVG(atraso_chegada_min), 2)                            AS atraso_chegada_medio,
  current_timestamp()                                          AS _processado_em
FROM {CATALOGO}.gold.fato_voos
WHERE partida_prevista_data IS NOT NULL
  AND COALESCE(suspeita_erro_horario, FALSE) = FALSE
  AND NOT etapa_duplicada_anac
  AND NOT duplicata_de_origem
GROUP BY 1,2,3,4
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.kpi_aeroporto_hora AS
SELECT
  icao_origem                                            AS icao_aeroporto,
  hora_brasilia,
  hora_local_origem,
  dia_semana,
  num_dia_semana,
  COUNT(*)                                               AS voos,
  ROUND(100 * AVG(CASE WHEN partida_pontual THEN 1.0 WHEN partida_pontual IS NOT NULL THEN 0 END), 2) AS pontualidade_pct,
  ROUND(AVG(atraso_partida_min), 2)                      AS atraso_partida_medio,
  ROUND(percentile_approx(atraso_partida_min, 0.9), 1)   AS atraso_partida_p90,
  current_timestamp()                                    AS _processado_em
FROM {CATALOGO}.gold.fato_voos
WHERE entra_em_pontualidade
  AND NOT dia_atipico
  AND COALESCE(suspeita_erro_horario, FALSE) = FALSE
  AND hora_brasilia IS NOT NULL
GROUP BY 1,2,3,4,5
""")

for t in ["kpi_diario", "kpi_rota_mensal", "kpi_aeroporto_hora"]:
    print(f"gold.{t}: {spark.table(f'{CATALOGO}.gold.{t}').count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### O efeito cascata, agora limpo
# MAGIC
# MAGIC Só voo programado, fora de dia atípico, sem horário suspeito. É o gráfico da seção *Horário do voo*
# MAGIC do dashboard: a taxa de atraso das partidas sobe de 8,0% às 5h para 25,9% às 22h (25,7% às 23h).
# MAGIC
# MAGIC **Como a média é feita.** Cada linha de `kpi_aeroporto_hora` é um grupo (aeroporto × hora × dia da
# MAGIC semana) com a pontualidade já calculada. Para juntar os grupos, a consulta pondera cada um pelo
# MAGIC número de voos. Os 306 grupos em que nenhuma partida tem horário real têm pontualidade nula; eles
# MAGIC ficam **fora do denominador** (uma versão anterior os contava como se fossem 0% pontuais, o que dava
# MAGIC 8,3% → 26,0%).
# MAGIC
# MAGIC Limite conhecido: o peso é `voos`, que inclui partidas sem horário real (canceladas). A conta
# MAGIC exata seria `SUM(partidas_pontuais) / SUM(partidas_avaliaveis)`, o que pede essas duas colunas em
# MAGIC `kpi_aeroporto_hora` (como já existem em `kpi_diario`). Fica para a próxima execução no Databricks.

# COMMAND ----------

display(spark.sql(f"""
    SELECT hora_brasilia,
           SUM(voos)                                              AS voos,
           -- denominador so com os grupos que tem valor: grupo com pontualidade nula (todas as
           -- partidas sem horario real) nao pode contar como 0% de pontualidade
           ROUND(SUM(voos * pontualidade_pct)
                 / SUM(CASE WHEN pontualidade_pct IS NOT NULL THEN voos END), 1)     AS pontualidade_pct,
           ROUND(SUM(voos * atraso_partida_medio)
                 / SUM(CASE WHEN atraso_partida_medio IS NOT NULL THEN voos END), 1) AS atraso_medio_min
    FROM {CATALOGO}.gold.kpi_aeroporto_hora
    GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## A OBT — a tabela que a IA lê
# MAGIC
# MAGIC Desnormalizada, nomes já resolvidos, métricas prontas, zero `JOIN`. Uma linha por etapa.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOGO}.gold.obt_voos AS
SELECT
  f._id_etapa,
  f.icao_empresa,  e.nome_companhia,  f.numero_voo,
  f.codigo_di,     cd.descricao      AS descricao_di,
  f.codigo_tipo_linha, cl.descricao  AS descricao_tipo_linha,
  f.escopo_voo, f.natureza_voo,

  f.icao_origem,  o.nome_aeroporto AS nome_aeroporto_origem,  o.praca_aeroporto AS praca_origem,
  o.uf_aeroporto AS uf_origem,  o.pais_aeroporto AS pais_origem,
  f.icao_destino, d.nome_aeroporto AS nome_aeroporto_destino, d.praca_aeroporto AS praca_destino,
  d.uf_aeroporto AS uf_destino, d.pais_aeroporto AS pais_destino,
  f.rota_icao,
  concat(COALESCE(o.praca_aeroporto, f.icao_origem), ' - ',
         COALESCE(d.praca_aeroporto, f.icao_destino))       AS rota_pracas,
  f.km_rota, f.faixa_etapa,

  f.partida_prevista_brasilia, f.partida_prevista_data, f.hora_brasilia,
  f.partida_prevista_local,    f.hora_local_origem,
  f.chegada_prevista_brasilia, f.chegada_prevista_local, f.hora_local_destino,
  f.partida_real_brasilia, f.chegada_real_brasilia,
  f.dia_semana, f.num_dia_semana, f.mes_referencia,

  f.atraso_partida_min, f.atraso_chegada_min, f.minutos_recuperados,
  f.duracao_programada_min, f.duracao_real_min, f.velocidade_programada_kmh,
  f.partida_pontual, f.chegada_pontual, f.classe_atraso, f.resolveu_atraso,

  f.situacao_voo, f.voo_realizado, f.voo_cancelado,
  f.voo_programado, f.entra_em_pontualidade, f.entra_em_cancelamento,
  f.etapa_duplicada_anac, f.duplicata_de_origem, f.voo_fantasma, f.cancelamento_operacional,
  f.dia_atipico, f.suspeita_erro_horario, f.destino_opera_a_noite,
  f.registro_malha_internacional,
  f._processado_em
FROM {CATALOGO}.gold.fato_voos f
LEFT JOIN {CATALOGO}.gold.dim_empresa   e ON e.icao_empresa   = f.icao_empresa
LEFT JOIN {CATALOGO}.gold.dim_aeroporto o ON o.icao_aeroporto = f.icao_origem
LEFT JOIN {CATALOGO}.gold.dim_aeroporto d ON d.icao_aeroporto = f.icao_destino
LEFT JOIN {CATALOGO}.silver.codigos_operacao cd ON cd.dominio = 'codigo_di'         AND cd.codigo = f.codigo_di
LEFT JOIN {CATALOGO}.silver.codigos_operacao cl ON cl.dominio = 'codigo_tipo_linha' AND cl.codigo = f.codigo_tipo_linha
""")

# contrato: a OBT nao pode ganhar nem perder linha nos JOINs
n_fato = spark.table(f"{CATALOGO}.gold.fato_voos").count()
n_obt  = spark.table(f"{CATALOGO}.gold.obt_voos").count()
assert n_fato == n_obt, f"JOIN alterou a contagem: fato={n_fato:,} obt={n_obt:,}"
print(f"gold.obt_voos: {n_obt:,} linhas (= fato_voos, nenhum JOIN duplicou)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
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

# COMMAND ----------

# MAGIC %md
# MAGIC # Governança
# MAGIC
# MAGIC Metadado, quando o consumidor é uma IA, deixa de ser documentação e vira **requisito funcional**.
# MAGIC O `COMMENT` é literalmente o texto que o modelo lê para escolher qual coluna usar.
# MAGIC
# MAGIC E tem um passo que não dá para pular: revisar cada descrição **contra o dado**. Abaixo, três frases
# MAGIC que parecem corretas e não batem com o dado — cada uma conferida por uma query.
# MAGIC
# MAGIC ### Frase 1 — *"`partida_prevista` está na hora local do aeroporto de origem."*

# COMMAND ----------

display(spark.sql(f"""
    SELECT rota_icao, COUNT(*) AS voos,
           ROUND(percentile_approx(duracao_programada_min, 0.5)) AS duracao_mediana_min
    FROM {CATALOGO}.gold.obt_voos
    WHERE rota_icao IN ('SBGR - SBEG', 'SBEG - SBGR', 'SBGR - SBCY', 'SBCY - SBGR')
    GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC GRU→MAO e MAO→GRU dão a **mesma** duração. Manaus é UTC−4 e Guarulhos é UTC−3: se cada horário
# MAGIC estivesse no relógio do seu próprio aeroporto, os dois sentidos difeririam em 2 horas.
# MAGIC Estão todos no mesmo relógio — o de Brasília.
# MAGIC
# MAGIC ### Frase 2 — *"`situacao_voo = CANCELADO` basta para medir a taxa de cancelamento."*

# COMMAND ----------

display(spark.sql(f"""
    SELECT nome_companhia, numero_voo,
           COUNT(*)                                                          AS ocorrencias,
           SUM(CASE WHEN voo_cancelado THEN 1 ELSE 0 END)                     AS cancelados,
           SUM(CASE WHEN partida_real_brasilia IS NOT NULL THEN 1 ELSE 0 END) AS chegou_a_partir
    FROM {CATALOGO}.gold.obt_voos
    WHERE voo_fantasma
    GROUP BY 1,2 ORDER BY ocorrencias DESC LIMIT 8
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Frase 3 — *"`minutos_recuperados > 0` significa que o voo chegou adiantado."*

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*)                                                 AS recuperou_algum_minuto,
           SUM(CASE WHEN atraso_chegada_min <= 0 THEN 1 ELSE 0 END) AS chegou_adiantado_ou_no_horario,
           SUM(CASE WHEN atraso_chegada_min >  0 THEN 1 ELSE 0 END) AS chegou_atrasado_mesmo_assim,
           SUM(CASE WHEN atraso_chegada_min > 15 THEN 1 ELSE 0 END) AS atrasado_mais_de_15
    FROM {CATALOGO}.gold.obt_voos
    WHERE minutos_recuperados > 0
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Recuperar tempo no ar não é chegar no horário. A diferença entre "recuperou" e "resolveu" é uma
# MAGIC coluna inteira — `resolveu_atraso`. As três frases estão corrigidas nos comentários abaixo.

# COMMAND ----------

COMENTARIOS_OBT = {
    "_id_etapa": "Identificador unico da etapa: hash das colunas de identificacao mais os horarios. E a chave de carga incremental. A chave de negocio (empresa+voo+rota+previsto) NAO e unica: ha 137 colisoes.",

    "icao_empresa":   "Codigo ICAO de tres letras da companhia que operou a etapa. Use nome_companhia para exibir; este codigo serve para filtro exato.",
    "nome_companhia": "Razao social da companhia. Quando o codigo nao existe no cadastro da ANAC, traz COMPANHIA NAO CADASTRADA seguida do codigo, em vez de vazio. Ocorre em 7 codigos e 69 voos.",
    "numero_voo":     "Numero comercial do voo. NAO e identificador unico: repete todos os dias, tem zero a esquerda e alguns comecam com letra.",

    "codigo_di":            "Codigo de autorizacao (DI) da etapa. E o campo que diz se o voo era PROGRAMADO: DI=0 tem horario previsto em 100 por cento das linhas, e nenhum outro DI tem. O codigo 1 aparece no dado e nao consta na tabela oficial da ANAC.",
    "descricao_di":         "Tipo da etapa por extenso: regular, extra, de retorno, charter, fretamento, duplicada. Quando o codigo nao esta catalogado pela ANAC, diz isso explicitamente.",
    "codigo_tipo_linha":    "Codigo do tipo de linha da ANAC: N e C domesticas, I e G internacionais.",
    "descricao_tipo_linha": "Tipo de linha por extenso: Domestica Mista, Internacional Cargueira, etc.",
    "escopo_voo":           "Domestico ou Internacional, derivado do tipo de linha. E a coluna certa para comparar os dois universos.",
    "natureza_voo":         "Misto (passageiros) ou Cargueiro. Cargueiro tem perfil de horario e de cancelamento muito diferente: nao misture na mesma media.",

    "icao_origem":            "Codigo ICAO do aeroporto de partida. Use nome_aeroporto_origem para exibir.",
    "nome_aeroporto_origem":  "Nome do aeroporto de partida. Aeroporto estrangeiro nao consta no cadastro da ANAC e aparece como AEROPORTO FORA DO CADASTRO ANAC seguido do codigo.",
    "praca_origem":           "Municipio SERVIDO pelo aeroporto de partida, que pode ser diferente do municipio onde ele fica. E a coluna certa para analisar praca; municipio fisico agrupa errado.",
    "uf_origem":              "Unidade federativa do aeroporto de partida, POR EXTENSO (Sao Paulo, Ceara), como a ANAC publica. Nao e a sigla: WHERE uf_origem = 'SP' devolve zero linhas.",
    "pais_origem":            "Brasil ou Exterior, deduzido do prefixo do codigo ICAO.",
    "icao_destino":           "Codigo ICAO do aeroporto de chegada. Use nome_aeroporto_destino para exibir.",
    "nome_aeroporto_destino": "Nome do aeroporto de chegada. Mesma regra de fallback da origem.",
    "praca_destino":          "Municipio servido pelo aeroporto de chegada.",
    "uf_destino":             "Unidade federativa do aeroporto de chegada, por extenso.",
    "pais_destino":           "Brasil ou Exterior para o aeroporto de chegada.",
    "rota_icao":              "Rota no formato ORIGEM - DESTINO usando codigos ICAO. Chave estavel para agrupar por rota; a rota inversa e uma rota diferente.",
    "rota_pracas":            "Rota em nomes de praca, para leitura humana. Aeroporto estrangeiro aparece pelo codigo ICAO.",
    "km_rota":                "Distancia de grande circulo entre origem e destino, em quilometros, calculada das coordenadas da ANAC. Nula quando alguma ponta nao esta no cadastro - cobre 80,8 por cento das etapas.",
    "faixa_etapa":            "curta (<400 km), media (400-1000), longa (1000-2000), muito_longa (>2000). Atraso de 20 minutos numa ponte aerea e outro fenomeno que 20 minutos num voo intercontinental: compare dentro da mesma faixa.",

    "partida_prevista_brasilia": "Horario programado de partida em HORA DE BRASILIA (UTC-3). NAO e a hora local do aeroporto: todos os horarios do VRA usam um relogio unico. Verificado por ida e volta terem a mesma duracao em rotas que cruzam fuso.",
    "partida_prevista_data":     "Data programada da partida, hora de Brasilia. Use para serie diaria e recorte de periodo.",
    "hora_brasilia":             "Hora cheia (0-23) da partida programada em hora de Brasilia. E a coluna certa para analisar propagacao de atraso pela REDE nacional ao longo do dia, porque coloca todos os aeroportos no mesmo relogio.",
    "partida_prevista_local":    "Horario programado de partida convertido para a HORA LOCAL do aeroporto de origem. Nulo para aeroporto estrangeiro, cujo fuso varia com horario de verao e nao foi assumido.",
    "hora_local_origem":         "Hora cheia (0-23) da partida no relogio de quem esta no aeroporto de origem. E a coluna certa para perguntar o horario de pico de UM aeroporto. Difere de hora_brasilia em 42.235 etapas (4,8 por cento), as que partem dos 79 aerodromos fora do UTC-3: o pico de Manaus sai de 12h para 11h e o de Rio Branco de 02h para 00h.",
    "chegada_prevista_brasilia": "Horario programado de chegada, hora de Brasilia. Nao e a hora local do destino.",
    "chegada_prevista_local":    "Horario programado de chegada na hora local do aeroporto de destino. Nulo para destino estrangeiro.",
    "hora_local_destino":        "Hora cheia da chegada no relogio do aeroporto de destino.",
    "partida_real_brasilia":     "Horario em que a aeronave efetivamente partiu, hora de Brasilia. Nulo em voo cancelado.",
    "chegada_real_brasilia":     "Horario em que a aeronave efetivamente pousou, hora de Brasilia. Nulo em voo cancelado.",
    "dia_semana":                "Dia da semana da partida programada, por extenso e em minusculas.",
    "num_dia_semana":            "Numero do dia da semana (1 = domingo), para ordenar graficos.",
    "mes_referencia":            "Primeiro dia do mes da partida programada, para agregacao mensal. Nulo nos voos sem horario previsto, que sao todos voos nao programados (DI diferente de 0).",

    "atraso_partida_min":        "Atraso de partida em minutos: real menos programado. Negativo significa que saiu adiantado. Nulo em voo cancelado ou sem horario programado.",
    "atraso_chegada_min":        "Atraso de chegada em minutos: real menos programado. Negativo significa que pousou adiantado.",
    "minutos_recuperados":       "Atraso de partida menos atraso de chegada. Positivo significa que a etapa chegou MENOS ATRASADA do que saiu - e NAO que chegou no horario: 164 mil voos recuperaram tempo e ainda assim pousaram atrasados. Para saber se resolveu, use resolveu_atraso.",
    "duracao_programada_min":    "Minutos entre partida e chegada programadas. Como os dois horarios estao no mesmo relogio, este valor e o tempo de bloco real, sem distorcao de fuso.",
    "duracao_real_min":          "Minutos entre partida e chegada efetivas.",
    "velocidade_programada_kmh": "km_rota dividido pela duracao programada. Mediana 498 km/h. Serve como controle de qualidade fisico, nao como metrica de negocio.",
    "partida_pontual":           "Verdadeiro quando a partida atrasou 15 minutos ou menos. Nulo quando NAO DA para avaliar: voo cancelado, sem horario programado, ou nao programado (DI diferente de 0). Nulo nunca deve ser contado como atraso.",
    "chegada_pontual":           "Verdadeiro quando a chegada atrasou 15 minutos ou menos. Mesma regra de nulo da partida.",
    "classe_atraso":             "Faixa do atraso de chegada: no_horario (ate 15 min), leve (ate 30), moderado (ate 60), severo (ate 120), critico (acima). Existe porque um booleano trata 16 minutos e 4 horas como a mesma coisa.",
    "resolveu_atraso":           "Verdadeiro quando a etapa recuperou tempo no ar E chegou pontual. E a coluna que responde se o atraso foi resolvido - minutos_recuperados sozinho nao responde isso.",

    "situacao_voo":              "Situacao informada pela companhia: REALIZADO ou CANCELADO. Leia junto com voo_fantasma: 23,4 por cento dos CANCELADO pertencem a voos-fantasma (regra R2).",
    "voo_realizado":             "Verdadeiro quando a etapa foi realizada. Use como denominador de metricas operacionais.",
    "voo_cancelado":             "Verdadeiro quando a companhia declarou cancelamento. Para taxa de cancelamento use cancelamento_operacional, que exclui voo-fantasma.",
    "voo_programado":            "Verdadeiro quando DI=0, ou seja, o voo constava da malha programada. Regra R1. So voo programado tem horario previsto - zero excecoes em 974.802 linhas.",
    "entra_em_pontualidade":     "Filtro oficial do denominador de PONTUALIDADE: voo programado (DI=0) e primeira ocorrencia da linha. Voo extra ou de retorno nao tinha horario para cumprir, e linha repetida pela fonte nao e um segundo voo.",
    "entra_em_cancelamento":     "Filtro oficial do denominador de CANCELAMENTO: DI 0 ou 2, e primeira ocorrencia da linha. Nos demais DI a taxa de cancelamento e exatamente 0,00 por cento - voo nao programado nao pode ser cancelado, so existe no dado porque aconteceu.",
    "etapa_duplicada_anac":      "Verdadeiro quando DI='D', codigo com que a propria ANAC marca Etapa de Voo Duplicada. 694 linhas, todas em cargueiras. Nao conte como voo.",
    "duplicata_de_origem":       "Verdadeiro na SEGUNDA ocorrencia de uma linha que a ANAC publicou repetida: 42 pares rigorosamente identicos (mesma companhia, voo, rota, horario previsto e real). Diferente de etapa_duplicada_anac, que e uma marcacao da propria fonte. A linha fica na tabela, mas sai do denominador via entra_em_pontualidade e entra_em_cancelamento.",
    "voo_fantasma":              "Regra R2. Verdadeiro quando o par companhia+numero_voo tem 30 ocorrencias ou mais no periodo E 98 por cento ou mais delas canceladas. Sao 54 pares e 6.813 linhas (0,67 por cento da base) que respondem por 23,4 por cento dos cancelamentos do pais. Apenas 2 dessas linhas tem partida real. O dado nao informa o motivo desses registros.",
    "cancelamento_operacional":  "Verdadeiro quando houve cancelamento de verdade: CANCELADO e NAO voo-fantasma. E a coluna para qualquer taxa de cancelamento publicada. Usando esta em vez de voo_cancelado, a Iberia sai de 54,61 por cento para 6,46 por cento.",
    "dia_atipico":               "Regra R5. Verdadeiro quando a taxa nacional de cancelamento do dia passou de 3 vezes a mediana do periodo (2,72 por cento). Separa evento sistemico de desempenho da companhia: em 10/12/2025 foram cancelados 17,85 por cento dos voos.",
    "suspeita_erro_horario":     "Regra R4. Verdadeiro quando a velocidade media programada fica fora de 200-950 km/h, faixa que o projeto considera plausivel; sugere erro de horario ou de data na origem. 7.532 voos. Pega o que a faixa de atraso -2h a +24h deixa passar.",
    "destino_opera_a_noite":     "Falso quando o aeroporto de destino nao tem operacao noturna (130 aerodromos no cadastro da ANAC, 17 entre os aeroportos do VRA). A taxa de cancelamento dos voos para esses destinos e 22,36 por cento, contra 2,87 por cento geral; o dado nao informa o motivo.",
    "registro_malha_internacional": "Verdadeiro quando o registro veio do sistema de malha internacional, identificado pela fracao de segundo no horario previsto do arquivo. E LINHAGEM, nao qualidade: a Turkish tem 98,8 por cento dos registros assim e cancela so 1,44 por cento. Nao use como filtro de confianca - use voo_fantasma.",
    "_processado_em":            "Auditoria: momento em que esta linha foi construida na camada gold.",
}

colunas_obt = {c.name for c in spark.table(f"{CATALOGO}.gold.obt_voos").schema}
n = 0
for coluna, comentario in COMENTARIOS_OBT.items():
    if coluna in colunas_obt:
        spark.sql(f"ALTER TABLE {CATALOGO}.gold.obt_voos ALTER COLUMN {coluna} COMMENT '{sql_literal(comentario)}'")
        n += 1
print(f"{n} de {len(colunas_obt)} colunas comentadas em gold.obt_voos")

# o fato herda as descricoes da OBT onde a coluna e a mesma
cols_fato = {c.name for c in spark.table(f"{CATALOGO}.gold.fato_voos").schema}
n = 0
for coluna, comentario in COMENTARIOS_OBT.items():
    if coluna in cols_fato:
        spark.sql(f"ALTER TABLE {CATALOGO}.gold.fato_voos ALTER COLUMN {coluna} COMMENT '{sql_literal(comentario)}'")
        n += 1
print(f"{n} colunas comentadas em gold.fato_voos")

# COMMAND ----------

COMENTARIOS_DIM_AEROPORTO = {
    "icao_aeroporto":      "Codigo ICAO do aeroporto. Chave da dimensao, serve para origem e destino do fato.",
    "nome_aeroporto":      "Nome do aeroporto, com fallback textual quando nao esta no cadastro da ANAC.",
    "municipio_aeroporto": "Municipio onde o aeroporto esta fisicamente localizado.",
    "praca_aeroporto":     "Municipio SERVIDO pelo aeroporto. E o certo para agrupar por praca.",
    "uf_aeroporto":        "Unidade federativa por extenso, como a ANAC publica. Nao e a sigla.",
    "latitude":            "Latitude em grau decimal. Usada para calcular km_rota no fato.",
    "longitude":           "Longitude em grau decimal.",
    "altitude_m":          "Altitude do aerodromo em metros.",
    "operacao_noturna":    "Capacidade de operacao noturna publicada pela ANAC, texto original.",
    "opera_a_noite":       "Regra de negocio derivada: falso quando operacao_noturna e 'Sem Operação'. 130 aerodromos no cadastro (17 entre os aeroportos do VRA). Nulo quando a ANAC nao publica a informacao.",
    "pais_aeroporto":      "Brasil ou Exterior, deduzido do prefixo ICAO.",
    "utc_offset":          "Fuso horario do aeroporto em horas em relacao ao UTC. Usado para converter os horarios do VRA, que estao em hora de Brasilia, para a hora local. NULO para aeroporto estrangeiro de proposito: eles tem horario de verao e um offset fixo seria aproximacao disfarcada de fato.",
    "no_cadastro_anac":    "Verdadeiro quando o aeroporto existe no cadastro de aerodromos publicos da ANAC. Falso e esperado para aeroporto estrangeiro; tambem e falso para 16 aeroportos brasileiros que estao no cadastro de PRIVADOS, fora do escopo deste projeto.",
    "_processado_em":      "Auditoria: momento da construcao da dimensao.",
}

COMENTARIOS_DIM_EMPRESA = {
    "icao_empresa":       "Codigo ICAO de tres letras da companhia. Chave da dimensao.",
    "nome_companhia":     "Razao social, com fallback textual quando o codigo nao tem cadastro.",
    "sigla_iata":         "Sigla IATA de duas letras.",
    "servico_autorizado": "Tipo de servico autorizado pela ANAC.",
    "cadastro_companhia": "De qual cadastro veio: nacional ou estrangeira. Nulo quando sem cadastro.",
    "no_cadastro_anac":   "Verdadeiro quando o codigo existe no cadastro da ANAC. Falso em 7 codigos e 69 voos.",
    "_processado_em":     "Auditoria: momento da construcao da dimensao.",
}

for tabela, mapa in [
    (f"{CATALOGO}.gold.dim_aeroporto", COMENTARIOS_DIM_AEROPORTO),
    (f"{CATALOGO}.gold.dim_empresa",   COMENTARIOS_DIM_EMPRESA),
]:
    cols = {c.name for c in spark.table(tabela).schema}
    for coluna, comentario in mapa.items():
        if coluna in cols:
            spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{sql_literal(comentario)}'")
    print(f"{len(mapa)} colunas comentadas em {tabela}")

# COMMAND ----------

TABELAS_GOLD = {
    f"{CATALOGO}.gold.obt_voos": (
        "Gold - One Big Table de voos da ANAC, desnormalizada e desenhada para consumo por agente de IA. "
        "Uma linha por etapa, nomes resolvidos e metricas prontas: responde as perguntas de negocio sem "
        "nenhum JOIN. Criterio de pontualidade: 15 minutos. Horarios em hora de Brasilia (UTC-3); a hora "
        "local do aeroporto esta nas colunas com sufixo _local. Antes de publicar qualquer taxa, aplique "
        "os filtros oficiais: entra_em_pontualidade, entra_em_cancelamento e cancelamento_operacional.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "ia", "grao": "etapa_de_voo", "padrao": "obt"},
    ),
    f"{CATALOGO}.gold.fato_voos": (
        "Gold - fato de voos, uma linha por etapa, com companhia e codigos de operacao como dimensoes "
        "degeneradas. E aqui que nascem as seis regras de negocio do projeto: populacao do KPI (R1), "
        "voo-fantasma (R2), hora local (R3), plausibilidade fisica (R4), dia atipico (R5). "
        "Contagem identica a silver.vra.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "bi", "grao": "etapa_de_voo", "padrao": "fato"},
    ),
    f"{CATALOGO}.gold.dim_aeroporto": (
        "Gold - dimensao de aeroporto, servindo origem e destino do fato. Construida a partir dos codigos "
        "presentes no fato e enriquecida pelo cadastro da ANAC, para cobrir 100 por cento do fato inclusive "
        "aeroportos estrangeiros. Traz coordenadas decimais, capacidade de operacao noturna e fuso horario.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "bi", "grao": "aeroporto", "padrao": "dimensao"},
    ),
    f"{CATALOGO}.gold.dim_empresa": (
        "Gold - dimensao de companhia aerea, construida a partir dos codigos presentes no fato e "
        "enriquecida pelo cadastro unificado da ANAC.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "bi", "grao": "empresa", "padrao": "dimensao"},
    ),
    f"{CATALOGO}.gold.kpi_diario": (
        "Gold - agregado diario por companhia e escopo, pronto para o dashboard. Ja exclui etapa duplicada "
        "e horario fisicamente implausivel. Traz numerador e denominador separados: calcule a taxa na "
        "leitura, nunca some percentuais ja calculados.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "dashboard", "grao": "dia_empresa"},
    ),
    f"{CATALOGO}.gold.kpi_rota_mensal": (
        "Gold - agregado mensal por rota e companhia, com o indice de confiabilidade (R6). Pesa "
        "pontualidade, cancelamento e o p90 do atraso: a media esconde a variabilidade, que e o que "
        "realmente afeta quem tem conexao. Minimo de 20 voos por grupo.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "dashboard", "grao": "rota_mes"},
    ),
    f"{CATALOGO}.gold.kpi_aeroporto_hora": (
        "Gold - agregado por aeroporto, hora e dia da semana, so com voo programado fora de dia atipico. "
        "Base do heatmap de efeito cascata. Traz hora de Brasilia e hora local lado a lado.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "dashboard", "grao": "aeroporto_hora"},
    ),
    f"{CATALOGO}.gold.voos_fantasma": (
        "Gold - pares companhia+numero_voo com 30 ou mais ocorrencias e 98 por cento ou mais canceladas (regra R2). "
        "Tabela de apoio: e o que permite mostrar no dashboard o ranking com e sem esses pares.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "qualidade", "grao": "empresa_voo"},
    ),
    f"{CATALOGO}.gold.dias_atipicos": (
        "Gold - taxa de cancelamento nacional por dia, com a marcacao de dia atipico (regra R5).",
        {"camada": "gold", "dominio": "aviacao", "consumo": "qualidade", "grao": "dia"},
    ),
}

for tabela, (comentario, tags) in TABELAS_GOLD.items():
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{sql_literal(comentario)}'")
    pares = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {tabela} SET TAGS ({pares})")
    print(f"{tabela}: comentario + {len(tags)} tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auditoria: 100% documentado?
# MAGIC
# MAGIC "Documentei tudo" é afirmação, não fato.

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_schema, table_name,
           COUNT(*)                                                         AS colunas,
           SUM(CASE WHEN comment IS NULL OR comment = '' THEN 1 ELSE 0 END) AS sem_comentario
    FROM {CATALOGO}.information_schema.columns
    WHERE table_schema IN ('silver', 'gold')
    GROUP BY 1,2 ORDER BY 1,2
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lineage — do arquivo cru até a OBT
# MAGIC
# MAGIC Ninguém escreveu uma linha de código de rastreabilidade. O Unity Catalog registrou sozinho quem leu
# MAGIC o quê para escrever o quê. É esse grafo que responde, em dez segundos, a pergunta mais cara de um
# MAGIC time de dados — *"se eu mexer aqui, o que quebra lá na frente?"* — e a do compliance, na direção
# MAGIC contrária: *"esse número no relatório veio de onde?"*

# COMMAND ----------

display(spark.sql(f"""
    SELECT COALESCE(nullif(source_table_full_name, ''), '(arquivo no volume)') AS origem,
           target_table_full_name                                             AS destino
    FROM system.access.table_lineage
    WHERE target_table_full_name LIKE '{CATALOGO}.%'
      AND event_date >= current_date() - 7
    GROUP BY 1, 2 ORDER BY destino, origem
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Saída para o MySQL e para o dashboard
# MAGIC
# MAGIC O Databricks Free Edition é serverless e **não alcança um MySQL rodando na sua máquina** — não
# MAGIC existe rota de rede entre os dois. A ponte é arquivo: exportamos no volume, você baixa por
# MAGIC *Catalog → Volumes → voebem → gold → export* e importa no Workbench.
# MAGIC
# MAGIC O que vai: as duas dimensões e os agregados. **A OBT de 1 milhão de linhas não vai** — ela fica no
# MAGIC Databricks servindo o agente de IA, e o dashboard não precisa dela.

# COMMAND ----------

PARA_EXPORTAR = [
    "dim_aeroporto",
    "dim_empresa",
    "kpi_diario",
    "kpi_rota_mensal",
    "kpi_aeroporto_hora",
    "voos_fantasma",
    "dias_atipicos",
]

for t in PARA_EXPORTAR:
    df = spark.table(f"{CATALOGO}.gold.{t}").drop("_processado_em")
    (df.coalesce(1).write.format("csv").mode("overwrite")
       .option("header", "true").option("sep", ",").option("nullValue", "")
       .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
       .save(f"{EXPORT}/{t}"))
    print(f"{t:22s} {df.count():>8,} linhas  ->  {EXPORT}/{t}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL do MySQL
# MAGIC
# MAGIC Gerado a partir do schema real das tabelas — assim o `CREATE TABLE` nunca sai de sincronia com o
# MAGIC que foi exportado. Copie a saída, cole no Workbench, rode, e depois importe os CSVs.

# COMMAND ----------

TIPO_MYSQL = {
    "StringType":    "VARCHAR(255)",
    "BooleanType":   "TINYINT(1)",
    "IntegerType":   "INT",
    "LongType":      "BIGINT",
    "DoubleType":    "DOUBLE",
    "FloatType":     "FLOAT",
    "DateType":      "DATE",
    "TimestampType": "DATETIME",
}

CHAVES = {
    "dim_aeroporto":      "PRIMARY KEY (icao_aeroporto)",
    "dim_empresa":        "PRIMARY KEY (icao_empresa)",
    "kpi_diario":         "KEY idx_dia (dia), KEY idx_emp (icao_empresa)",
    "kpi_rota_mensal":    "KEY idx_rota (rota_icao), KEY idx_mes (mes_referencia)",
    "kpi_aeroporto_hora": "KEY idx_apt (icao_aeroporto), KEY idx_hora (hora_brasilia)",
    "voos_fantasma":      "KEY idx_emp (icao_empresa)",
    "dias_atipicos":      "PRIMARY KEY (dia)",
}

ddl = ["CREATE DATABASE IF NOT EXISTS voebem CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
       "USE voebem;", ""]

for t in PARA_EXPORTAR:
    schema = spark.table(f"{CATALOGO}.gold.{t}").drop("_processado_em").schema
    linhas = []
    for campo in schema:
        nome_tipo = type(campo.dataType).__name__
        tipo = TIPO_MYSQL.get(nome_tipo, "VARCHAR(255)")
        if nome_tipo == "DecimalType":
            tipo = f"DECIMAL({campo.dataType.precision},{campo.dataType.scale})"
        linhas.append(f"  `{campo.name}` {tipo}")
    if t in CHAVES:
        linhas.append("  " + CHAVES[t])
    ddl.append(f"DROP TABLE IF EXISTS `{t}`;")
    ddl.append(f"CREATE TABLE `{t}` (\n" + ",\n".join(linhas) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
    ddl.append("")

ddl_texto = "\n".join(ddl)
print(ddl_texto)

dbutils.fs.put(f"{EXPORT}/schema_mysql.sql", ddl_texto, overwrite=True)
print(f"\n>>> DDL salvo em {EXPORT}/schema_mysql.sql")

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON para o dashboard
# MAGIC
# MAGIC Um arquivo só, com tudo que as páginas precisam. Página HTML não conversa com MySQL — ela lê este
# MAGIC JSON. Como são poucos milhares de linhas, carrega instantâneo e o dashboard funciona até offline.
# MAGIC
# MAGIC (O MySQL continua sendo a fonte de verdade e o lugar onde você escreve SQL de análise. O JSON é só
# MAGIC o recorte que a página precisa.)

# COMMAND ----------

def linhas_de(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]

painel = {
    "gerado_em": spark.sql("SELECT cast(current_timestamp() as string)").collect()[0][0],
    "regras": {
        "limiar_pontualidade_min":  LIMIAR_PONTUALIDADE_MIN,
        "fantasma_min_ocorrencias": FANTASMA_MIN_OCORRENCIAS,
        "fantasma_taxa_minima":     FANTASMA_TAXA_MINIMA,
        "velocidade_plausivel_kmh": [VEL_MIN_KMH, VEL_MAX_KMH],
        "atipico_fator":            ATIPICO_FATOR,
    },

    "resumo": linhas_de(f"""
        SELECT COUNT(*)                                                            AS registros,
               SUM(CASE WHEN entra_em_cancelamento THEN 1 ELSE 0 END)              AS voos_elegiveis,
               SUM(CASE WHEN voo_fantasma THEN 1 ELSE 0 END)                       AS linhas_fantasma,
               SUM(CASE WHEN etapa_duplicada_anac THEN 1 ELSE 0 END)               AS etapas_duplicadas,
               SUM(CASE WHEN duplicata_de_origem THEN 1 ELSE 0 END)                 AS duplicatas_de_origem,
               SUM(CASE WHEN suspeita_erro_horario THEN 1 ELSE 0 END)              AS horarios_suspeitos,
               ROUND(100*AVG(CASE WHEN partida_pontual THEN 1.0
                                  WHEN partida_pontual IS NOT NULL THEN 0 END), 2) AS pontualidade_partida_pct,
               ROUND(100*AVG(CASE WHEN chegada_pontual THEN 1.0
                                  WHEN chegada_pontual IS NOT NULL THEN 0 END), 2) AS pontualidade_chegada_pct
        FROM {CATALOGO}.gold.obt_voos"""),

    "cascata_por_hora": linhas_de(f"""
        SELECT hora_brasilia,
               SUM(voos)                                          AS voos,
               ROUND(SUM(voos*pontualidade_pct)
                     / SUM(CASE WHEN pontualidade_pct IS NOT NULL THEN voos END), 1)     AS pontualidade_pct,
               ROUND(SUM(voos*atraso_partida_medio)
                     / SUM(CASE WHEN atraso_partida_medio IS NOT NULL THEN voos END), 1) AS atraso_medio
        FROM {CATALOGO}.gold.kpi_aeroporto_hora GROUP BY 1 ORDER BY 1"""),

    "heatmap_hora_dia": linhas_de(f"""
        SELECT dia_semana, num_dia_semana, hora_brasilia,
               SUM(voos)                                      AS voos,
               ROUND(SUM(voos*pontualidade_pct)
                     / SUM(CASE WHEN pontualidade_pct IS NOT NULL THEN voos END), 1) AS pontualidade_pct
        FROM {CATALOGO}.gold.kpi_aeroporto_hora
        GROUP BY 1,2,3 ORDER BY 2,3"""),

    "ranking_companhias": linhas_de(f"""
        SELECT e.nome_companhia, f.icao_empresa,
               COUNT(*)                                                             AS registros,
               ROUND(100*AVG(CASE WHEN f.voo_cancelado THEN 1.0 ELSE 0 END), 2)      AS cancelamento_bruto,
               ROUND(100*AVG(CASE WHEN f.voo_fantasma THEN NULL
                                  WHEN f.voo_cancelado THEN 1.0 ELSE 0 END), 2)      AS cancelamento_real,
               ROUND(100*AVG(CASE WHEN f.partida_pontual THEN 1.0
                                  WHEN f.partida_pontual IS NOT NULL THEN 0 END), 2) AS pontualidade_pct
        FROM {CATALOGO}.gold.fato_voos f
        JOIN {CATALOGO}.gold.dim_empresa e USING (icao_empresa)
        GROUP BY 1,2 HAVING COUNT(*) >= 1000 ORDER BY registros DESC"""),

    "serie_mensal": linhas_de(f"""
        SELECT date_format(mes_referencia, 'yyyy-MM')                                     AS mes,
               COUNT(*)                                                                   AS voos,
               ROUND(100*AVG(CASE WHEN cancelamento_operacional THEN 1.0 ELSE 0 END), 2)  AS cancelamento_pct,
               ROUND(100*AVG(CASE WHEN chegada_pontual THEN 1.0
                                  WHEN chegada_pontual IS NOT NULL THEN 0 END), 2)        AS pontualidade_pct
        FROM {CATALOGO}.gold.fato_voos
        WHERE mes_referencia IS NOT NULL GROUP BY 1 ORDER BY 1"""),

    "rotas": linhas_de(f"""
        SELECT rota_icao, icao_empresa,
               SUM(voos_previstos)                     AS voos,
               MAX(km_rota)                            AS km,
               MAX(faixa_etapa)                        AS faixa,
               ROUND(AVG(100*pontualidade_chegada), 1) AS pontualidade_pct,
               ROUND(AVG(atraso_medio), 1)             AS atraso_medio,
               ROUND(AVG(atraso_p50), 1)               AS atraso_p50,
               ROUND(AVG(atraso_p90), 1)               AS atraso_p90,
               ROUND(AVG(indice_confiabilidade), 1)    AS indice
        FROM {CATALOGO}.gold.kpi_rota_mensal
        GROUP BY 1,2 HAVING SUM(voos_previstos) >= 100"""),

    "aeroportos": linhas_de(f"""
        SELECT a.icao_aeroporto, a.nome_aeroporto, a.praca_aeroporto, a.uf_aeroporto,
               a.latitude, a.longitude, a.utc_offset, a.opera_a_noite, a.pais_aeroporto,
               COUNT(*)                                                              AS voos,
               ROUND(100*AVG(CASE WHEN f.partida_pontual THEN 1.0
                                  WHEN f.partida_pontual IS NOT NULL THEN 0 END), 1) AS pontualidade_pct
        FROM {CATALOGO}.gold.fato_voos f
        JOIN {CATALOGO}.gold.dim_aeroporto a ON a.icao_aeroporto = f.icao_origem
        GROUP BY 1,2,3,4,5,6,7,8,9 HAVING COUNT(*) >= 100"""),

    "dias_atipicos": linhas_de(f"""
        SELECT cast(dia as string) AS dia, voos, cancelados, taxa_pct, dia_atipico
        FROM {CATALOGO}.gold.dias_atipicos ORDER BY dia"""),

    "voos_fantasma": linhas_de(f"""
        SELECT f.icao_empresa, e.nome_companhia, f.numero_voo,
               f.ocorrencias, f.cancelados, f.taxa_cancelamento
        FROM {CATALOGO}.gold.voos_fantasma f
        LEFT JOIN {CATALOGO}.gold.dim_empresa e USING (icao_empresa)
        ORDER BY f.ocorrencias DESC"""),
}

conteudo = json.dumps(painel, ensure_ascii=False, default=str)
dbutils.fs.put(f"{EXPORT}/painel.json", conteudo, overwrite=True)

print(f"painel.json gravado em {EXPORT}/painel.json")
print(f"tamanho: {len(conteudo)/1024:.1f} KB\n")
for k, v in painel.items():
    if isinstance(v, list):
        print(f"  {k:22s} {len(v):>6,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fechamento
# MAGIC
# MAGIC No volume `gold/export` estão:
# MAGIC
# MAGIC - **7 CSVs** — dimensões e agregados, prontos para importar no MySQL
# MAGIC - **`schema_mysql.sql`** — o `CREATE TABLE`, gerado do schema real
# MAGIC - **`painel.json`** — tudo que o dashboard precisa, num arquivo só
# MAGIC
# MAGIC A `obt_voos` fica aqui, com os `COMMENT` revisados contra o dado — é ela que o agente de IA vai
# MAGIC ler, e são os comentários que devem impedir o agente de responder que a Iberia cancela 55% dos voos.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOGO}.gold"))
