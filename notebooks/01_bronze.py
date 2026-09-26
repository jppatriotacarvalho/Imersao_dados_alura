# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — ingestão
# MAGIC
# MAGIC Cinco tabelas a partir dos arquivos no volume:
# MAGIC
# MAGIC | tabela | fonte |
# MAGIC |---|---|
# MAGIC | `bronze.vra` | 12 CSVs mensais do Voo Regular Ativo |
# MAGIC | `bronze.aerodromos` | `AerodromosPublicos.csv` |
# MAGIC | `bronze.empresas_nacionais` | `pda_empresas_aereas_nacionais.csv` |
# MAGIC | `bronze.empresas_estrangeiras` | `pda_empresas_aereas_estrangeiros.csv` |
# MAGIC | `ref.codigos_operacao` | seed curada à mão |
# MAGIC
# MAGIC **Regras da camada:** nada de tipagem, nada de filtro, colunas de auditoria, idempotente.
# MAGIC **Um arquivo, uma tabela** — unir dois cadastros é decisão de modelagem, e isso é trabalho da silver.
# MAGIC
# MAGIC Quatro decisões deste notebook que fogem do roteiro padrão, cada uma resolvendo um problema
# MAGIC que só aparece quando se olha o arquivo de perto:
# MAGIC
# MAGIC 1. **`_fonte_atualizada_em`** — a primeira linha de todo arquivo da ANAC é `Atualizado em: <data>`.
# MAGIC    O `skipRows=1` joga fora. É o único carimbo de frescor que a fonte dá.
# MAGIC 2. **`_marcador_registro`** — 79.793 linhas trazem o horário previsto com fração de segundo.
# MAGIC    Não é ruído: é a assinatura de outro sistema de origem, e ela morre no primeiro `cast`.
# MAGIC 3. **Reconciliação de linhas** — contamos as linhas físicas do arquivo e comparamos com a tabela.
# MAGIC    "Nada foi descartado" vira fato verificado.
# MAGIC 4. **A seed sai do bronze** — ela não foi ingerida, foi digitada. Vai para o schema `ref`.

# COMMAND ----------

from pyspark.sql import functions as F
import re

CATALOGO  = "voebem"
VOL_VRA   = f"/Volumes/{CATALOGO}/bronze/arquivos/vra/*.csv"
VOL_REF   = f"/Volumes/{CATALOGO}/bronze/arquivos/referencias"
SEM_ASPAS = chr(0)   # caractere que NAO existe no arquivo -> desliga o quoting do leitor

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.ref")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. O cabeçalho que a gente costuma jogar fora
# MAGIC
# MAGIC Todo arquivo da ANAC começa assim:
# MAGIC
# MAGIC ```
# MAGIC Atualizado em: 2026-08-28
# MAGIC "ICAO Empresa Aérea";"Número Voo";...
# MAGIC ```
# MAGIC
# MAGIC `_ingerido_em` responde *"quando eu li"*. `_fonte_atualizada_em` responde *"quando a ANAC
# MAGIC publicou"*. São perguntas diferentes, e só a segunda diz se o dado está velho. Os três arquivos
# MAGIC deste projeto têm datas de publicação **diferentes** — saber disso é a diferença entre "o
# MAGIC cadastro está desatualizado" e "achei que estava atualizado".

# COMMAND ----------

texto_vra = (
    spark.read.format("text").load(VOL_VRA)
    .withColumn("_arquivo_origem", F.col("_metadata.file_name"))
)

cabecalhos = (
    texto_vra.filter(F.col("value").rlike("Atualizado em"))
    .select(
        "_arquivo_origem",
        F.to_date(F.regexp_extract("value", r"(\d{4}-\d{2}-\d{2})", 1), "yyyy-MM-dd")
         .alias("_fonte_atualizada_em"),
    )
    .dropDuplicates(["_arquivo_origem"])
)

display(cabecalhos.orderBy("_arquivo_origem"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Contagem física — a testemunha independente
# MAGIC
# MAGIC Cada arquivo tem 1 linha de "Atualizado em" + 1 de cabeçalho + N de dado. Guardamos esse N
# MAGIC **antes** de qualquer parsing; no fim do notebook ele vira o teste.

# COMMAND ----------

linhas_fisicas = (
    texto_vra.groupBy("_arquivo_origem").count()
    .withColumn("linhas_de_dado", F.col("count") - 2)
    .drop("count")
)

total_esperado = linhas_fisicas.agg(F.sum("linhas_de_dado")).collect()[0][0]
print(f"linhas de dado esperadas nos arquivos: {total_esperado:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `bronze.vra` — leitura
# MAGIC
# MAGIC | opção | resolve |
# MAGIC |---|---|
# MAGIC | `sep=";"` | separador brasileiro |
# MAGIC | `skipRows=1` | descarta o "Atualizado em" (e o BOM, que mora nessa linha) |
# MAGIC | `header=true` | a segunda linha é o cabeçalho de verdade |
# MAGIC | `quote` + `escape` | campos entre aspas; `escape` protege aspas internas |
# MAGIC | sem `inferSchema` | bronze não tipa: tudo string |
# MAGIC | `mode=PERMISSIVE` | bronze não descarta linha nenhuma |

# COMMAND ----------

bruto = (
    spark.read.format("csv")
    .option("sep", ";").option("header", "true").option("skipRows", 1)
    .option("quote", '"').option("escape", '"')
    .option("encoding", "UTF-8").option("mode", "PERMISSIVE")
    .load(VOL_VRA)
)

print("colunas lidas do arquivo:")
for c in bruto.columns:
    print(f"  {c!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nomes de coluna
# MAGIC
# MAGIC `ICAO Empresa Aérea` é nome válido em CSV e **inválido** em Delta — espaço está na lista de
# MAGIC caracteres proibidos. Normalizamos o **nome**: o que a bronze preserva é o **valor** e a
# MAGIC **granularidade**, não a grafia do cabeçalho.
# MAGIC
# MAGIC Mapa explícito, sem `regexp_replace` mágico — a correspondência com o arquivo original precisa
# MAGIC ser auditável linha a linha.

# COMMAND ----------

RENOMEAR = {
    "ICAO Empresa Aérea":      "icao_empresa",
    "Número Voo":              "numero_voo",
    "Código Autorização (DI)": "codigo_di",
    "Código Tipo Linha":       "codigo_tipo_linha",
    "ICAO Aeródromo Origem":   "icao_origem",
    "ICAO Aeródromo Destino":  "icao_destino",
    "Partida Prevista":        "partida_prevista",
    "Partida Real":            "partida_real",
    "Chegada Prevista":        "chegada_prevista",
    "Chegada Real":            "chegada_real",
    "Situação Voo":            "situacao_voo",
    "Código Justificativa":    "codigo_justificativa",
}

faltando = [c for c in RENOMEAR if c not in bruto.columns]
assert not faltando, f"Coluna esperada nao encontrada no CSV: {faltando}"

renomeado = bruto.select(
    *[F.col(f"`{o}`").cast("string").alias(n) for o, n in RENOMEAR.items()]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### O marcador que o `cast` destrói
# MAGIC
# MAGIC Dois formatos de timestamp convivem no mesmo arquivo:
# MAGIC
# MAGIC ```
# MAGIC 2026-01-27 19:45:00
# MAGIC 2026-01-27 19:45:00.100000000     <- 79.793 linhas
# MAGIC ```
# MAGIC
# MAGIC O caminho óbvio é resolver com `try_cast` na silver e seguir a vida. Só que a fração
# MAGIC **não é aleatória**:
# MAGIC
# MAGIC - aparece **só** nos horários *previstos*, nunca nos *reais*
# MAGIC - 90% das linhas com ela são internacionais (72% tipo I + 18% tipo G)
# MAGIC - as companhias são quase todas estrangeiras: Copa, Iberia, American, Qatar, Turkish
# MAGIC
# MAGIC É assinatura de **sistema de origem**: esses registros vêm do fluxo de malha internacional, não
# MAGIC do fluxo operacional doméstico. Metadado de linhagem de graça — que deixa de existir no instante
# MAGIC em que alguém faz o cast.
# MAGIC
# MAGIC Capturamos como booleano. **Sem interpretar nada**: a bronze registra que o marcador existe;
# MAGIC o que ele significa é decisão de negócio e mora na gold.

# COMMAND ----------

bronze_vra = (
    renomeado
    .withColumn("_arquivo_origem", F.col("_metadata.file_name"))
    .withColumn("_marcador_registro", F.col("partida_prevista").contains(".").cast("boolean"))
    .join(cabecalhos, on="_arquivo_origem", how="left")
    .withColumn("_ingerido_em", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escrita idempotente
# MAGIC
# MAGIC **Full refresh determinístico** — `overwrite` sobre o conjunto inteiro de arquivos.
# MAGIC
# MAGIC 1. A fonte é tratada como completa: cada execução relê os arquivos mensais inteiros.
# MAGIC    A entrada define o estado final, logo o destino pode ser derivado inteiro dela.
# MAGIC 2. `append` exigiria chave de negócio para deduplicar — e o VRA **não tem chave natural única**
# MAGIC    (medido: 137 colisões). Deduplicar aqui seria decidir regra de negócio na camada errada.
# MAGIC 3. `overwrite` no Delta é atômico: ninguém lê tabela pela metade.
# MAGIC 4. O histórico não se perde — cada `overwrite` gera versão nova no log, acessível por time travel.

# COMMAND ----------

(bronze_vra.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOGO}.bronze.vra"))

total_gravado = spark.table(f"{CATALOGO}.bronze.vra").count()
print(f"bronze.vra: {total_gravado:,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconciliação — a prova de que nada se perdeu
# MAGIC
# MAGIC A silver vai provar `bronze == silver`. Mas quem prova `arquivo == bronze`?
# MAGIC
# MAGIC Uma linha mal formada não gera erro no modo PERMISSIVE — ela entra torta, ou o parser engole a
# MAGIC próxima. O único jeito de saber é contar as linhas do arquivo **por fora do parser**.

# COMMAND ----------

conferencia = (
    spark.table(f"{CATALOGO}.bronze.vra").groupBy("_arquivo_origem").count()
    .withColumnRenamed("count", "linhas_na_tabela")
    .join(linhas_fisicas, on="_arquivo_origem", how="full_outer")
    .withColumn("diferenca", F.col("linhas_na_tabela") - F.col("linhas_de_dado"))
    .orderBy("_arquivo_origem")
)
display(conferencia)

divergentes = conferencia.filter(F.col("diferenca") != 0).count()
assert divergentes == 0, f"{divergentes} arquivo(s) com contagem divergente - investigar antes de seguir"
print(f"OK: {total_gravado:,} linhas na tabela = {total_esperado:,} linhas nos arquivos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `bronze.aerodromos` — latin-1, aspas que não são aspas, e 7 colunas que ninguém usa
# MAGIC
# MAGIC Duas armadilhas de leitura:
# MAGIC
# MAGIC **`encoding = "ISO-8859-1"`.** Este CSV não é UTF-8. Lido como UTF-8, "Plácido de Castro" vira
# MAGIC "Pl?cido de Castro" — e o nome quebrado chega no produto final, que é justamente o que o cliente lê.
# MAGIC
# MAGIC **`quote = chr(0)`.** O arquivo **não usa** aspas para delimitar campo, mas usa `"` como símbolo
# MAGIC de *segundo* nas coordenadas: `09°52'06"S`. Com o `quote='"'` padrão, o Spark abre uma aspa ali e
# MAGIC sai engolindo linhas até achar a próxima. Apontar o quoting para um caractere que não existe no
# MAGIC arquivo é o que mantém uma linha = um registro.
# MAGIC
# MAGIC E uma decisão de modelagem: **levamos o arquivo inteiro, não um recorte.** O impulso natural é
# MAGIC pegar só ICAO, nome, município e UF. Quatro colunas normalmente descartadas valem muito:
# MAGIC
# MAGIC | coluna | por quê |
# MAGIC |---|---|
# MAGIC | `LATGEOPOINT` / `LONGEOPOINT` | latitude e longitude **em grau decimal**, 496/496 preenchidas. O `Latitude`/`Longitude` que se costuma levar é DMS (`09°52'06"S`): string, não serve para conta nenhuma. Com o decimal, dá para calcular distância de rota. |
# MAGIC | `Operação Noturna` | 130 aeródromos são "Sem Operação". Guardar a coluna permite cruzar operação noturna com atraso e cancelamento. |
# MAGIC | `Município Servido` | o município que o aeroporto **atende** ≠ onde ele fica. É o certo para agrupar por praça. |
# MAGIC | `Situação` / `Validade do Registro` | vigência do cadastro. |
# MAGIC
# MAGIC Nenhuma delas é regra de negócio. São dado que já veio no arquivo e costuma ser jogado fora.

# COMMAND ----------

def data_de_publicacao(arquivo: str):
    """Le a primeira linha do CSV e extrai a data de 'Atualizado em: yyyy-MM-dd'."""
    primeira = spark.read.text(f"{VOL_REF}/{arquivo}").limit(1).collect()
    if not primeira:
        return None
    m = re.search(r"(\d{4}-\d{2}-\d{2})", primeira[0]["value"])
    return m.group(1) if m else None


def marcar_auditoria(df, arquivo: str):
    return (df.withColumn("_arquivo_origem", F.lit(arquivo))
              .withColumn("_fonte_atualizada_em", F.to_date(F.lit(data_de_publicacao(arquivo))))
              .withColumn("_ingerido_em", F.current_timestamp()))

# COMMAND ----------

ARQ_AERO = "AerodromosPublicos.csv"

aerodromos = marcar_auditoria(
    spark.read.format("csv")
    .option("sep", ";").option("header", "true").option("skipRows", 1)
    .option("encoding", "ISO-8859-1")   # latin-1, nao UTF-8
    .option("quote", SEM_ASPAS)         # aspas aqui sao "segundos", nao delimitador
    .load(f"{VOL_REF}/{ARQ_AERO}")
    .select(
        F.col("`Código OACI`").alias("icao"),
        F.col("CIAD").alias("ciad"),
        F.col("Nome").alias("nome"),
        F.col("`Município`").alias("municipio"),
        F.col("UF").alias("uf"),
        F.col("`Município Servido`").alias("municipio_servido"),
        F.col("`UF Servido`").alias("uf_servido"),
        F.col("LATGEOPOINT").alias("latitude_decimal"),     # <- o ouro
        F.col("LONGEOPOINT").alias("longitude_decimal"),    # <- o ouro
        F.col("Latitude").alias("latitude_dms"),
        F.col("Longitude").alias("longitude_dms"),
        F.col("Altitude").alias("altitude"),
        F.col("`Operação Diurna`").alias("operacao_diurna"),
        F.col("`Operação Noturna`").alias("operacao_noturna"),
        F.col("`Situação`").alias("situacao"),
        F.col("`Validade do Registro`").alias("validade_registro"),
    ),
    ARQ_AERO,
)

(aerodromos.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.bronze.aerodromos"))

print(f"bronze.aerodromos: {spark.table(f'{CATALOGO}.bronze.aerodromos').count():,} linhas "
      f"(publicado em {data_de_publicacao(ARQ_AERO)})")

display(spark.sql(f"""
    SELECT icao, nome, municipio, municipio_servido, uf,
           latitude_decimal, longitude_decimal, operacao_noturna
    FROM {CATALOGO}.bronze.aerodromos
    WHERE icao IN ('SBRB','SBGR','SBSP','SBFZ','SWBC')
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limitação conhecida deste cadastro
# MAGIC
# MAGIC Medido: **234 dos 396 aeroportos do VRA não estão neste arquivo** — 10,4% de todas as pontas de
# MAGIC voo. A explicação usual é "são os estrangeiros, a ANAC não cadastra", e ela cobre a maioria.
# MAGIC
# MAGIC Mas **16 são brasileiros**, com 2.983 pontas de voo:
# MAGIC
# MAGIC ```
# MAGIC SNCL 1.198   SBUY 1.032   SSOU 555   SDLO 151   SSCE 8   SSBZ 6   SBGP 6 ...
# MAGIC ```
# MAGIC
# MAGIC Eles existem no cadastro da ANAC, só que no arquivo de **aeródromos privados**, que não faz parte
# MAGIC deste projeto. Ou seja: o rótulo "fora do cadastro ANAC" está tecnicamente certo aqui e
# MAGIC conceitualmente incompleto — não é que a ANAC não os conheça, é que esta fonte não os cobre.
# MAGIC
# MAGIC **Fica registrado como limitação conhecida**, não escondido. Saber onde o dado acaba é parte
# MAGIC de entregar o dado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Empresas — dois cadastros, duas tabelas. E as 5 linhas que o PERMISSIVE engole
# MAGIC
# MAGIC A ANAC publica empresas aéreas em dois arquivos, de dois processos administrativos diferentes.
# MAGIC Os dois têm **exatamente** o mesmo cabeçalho, então um `UNION` sairia de graça — e é exatamente
# MAGIC por isso que não se faz: se amanhã a ANAC republicar só as estrangeiras, eu quero reprocessar só
# MAGIC elas. A união é decisão de modelagem e o lugar dela é a silver.
# MAGIC
# MAGIC Agora o problema de verdade. O cadastro nacional tem **aspas não escapadas dentro de campo**:
# MAGIC
# MAGIC ```
# MAGIC "AV. PRESIDENTE JOÃO GOULART, Nº 660, SALA "B" - BAIRRO RODOVIÁRIA"
# MAGIC "AVENIDA ALMIRANTE JÚLIO DE SÁ BIERRENBACH , N". 65 - BLOCO 3"
# MAGIC ```
# MAGIC
# MAGIC São 5 linhas (L297, L405, L471, L495, L631). O modo PERMISSIVE **não rejeita**: os campos deslizam
# MAGIC e a linha entra torta, sem erro. Dá para ver o estrago no resultado — no campo `Ativa` aparecem
# MAGIC valores como `;` e `5/6/2022 12:00:00 AM`.
# MAGIC
# MAGIC Auditar "100% das colunas comentadas" e não auditar "0% das linhas corrompidas" é olhar para o
# MAGIC lugar errado.
# MAGIC
# MAGIC **Como detectar sem depender do parser:** contar campos no texto cru. Uma linha bem formada tem
# MAGIC 13 campos, ou seja 12 ocorrências do separador `";"`. Quem foge disso, foge.

# COMMAND ----------

CAMPOS_ESPERADOS = 13
ARQ_NAC = "pda_empresas_aereas_nacionais.csv"
ARQ_EST = "pda_empresas_aereas_estrangeiros.csv"


def linhas_malformadas(arquivo: str):
    """Conta campos no texto cru, sem passar pelo parser de CSV."""
    return (
        spark.read.text(f"{VOL_REF}/{arquivo}")
        .withColumn("_linha", F.monotonically_increasing_id() + 1)
        .filter(F.col("value").startswith('"'))                 # ignora "Atualizado em"
        .withColumn("campos", F.size(F.split(F.col("value"), '";"')))
        .filter(F.col("campos") != CAMPOS_ESPERADOS)
        .select(F.lit(arquivo).alias("_arquivo_origem"), "_linha", "campos",
                F.col("value").alias("conteudo"))
    )


(linhas_malformadas(ARQ_NAC).unionByName(linhas_malformadas(ARQ_EST))
 .withColumn("_detectado_em", F.current_timestamp())
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOGO}.bronze.referencias_malformadas"))

n_ruins = spark.table(f"{CATALOGO}.bronze.referencias_malformadas").count()
print(f"bronze.referencias_malformadas: {n_ruins} linha(s) fora do formato esperado")
display(spark.sql(f"""
    SELECT _arquivo_origem, campos, substr(conteudo, 1, 160) AS inicio_da_linha
    FROM {CATALOGO}.bronze.referencias_malformadas
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC As 5 linhas continuam entrando na tabela — bronze não descarta nada. A diferença é que agora
# MAGIC elas estão **nomeadas**: existe uma tabela que diz quais são, e a silver pode decidir o que fazer.
# MAGIC
# MAGIC (Neste snapshot todas as 5 são empresas sem código ICAO — aeroagrícola e táxi aéreo — então não
# MAGIC afetam nenhum join hoje. "Hoje" é a palavra importante.)

# COMMAND ----------

def ler_empresas(arquivo: str):
    return marcar_auditoria(
        spark.read.format("csv")
        .option("sep", ";").option("header", "true").option("skipRows", 1)
        .option("encoding", "UTF-8")
        .option("quote", '"').option("escape", '"')   # escape: a defesa contra as aspas internas
        .option("mode", "PERMISSIVE")
        .load(f"{VOL_REF}/{arquivo}")
        .select(
            F.col("ICAO").alias("icao"),
            F.col("Estrangeira").alias("sigla_iata"),
            F.col("Razao").alias("razao_social"),
            F.col("Servico").alias("servico"),
            F.col("Cidade").alias("cidade"),
            F.col("UF").alias("uf"),
            F.col("Ativa").alias("situacao"),
        ),
        arquivo,
    )


for arquivo, tabela in [
    (ARQ_NAC, f"{CATALOGO}.bronze.empresas_nacionais"),
    (ARQ_EST, f"{CATALOGO}.bronze.empresas_estrangeiras"),
]:
    (ler_empresas(arquivo).write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(tabela))
    print(f"{tabela}: {spark.table(tabela).count():,} linhas  (publicado em {data_de_publicacao(arquivo)})")

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'empresas_nacionais' AS tabela, COUNT(*) AS linhas,
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END) AS com_icao,
           MAX(_fonte_atualizada_em) AS publicado_em
    FROM {CATALOGO}.bronze.empresas_nacionais
    UNION ALL
    SELECT 'empresas_estrangeiras', COUNT(*),
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END),
           MAX(_fonte_atualizada_em)
    FROM {CATALOGO}.bronze.empresas_estrangeiras
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Repare nas datas: os dois cadastros foram publicados em meses diferentes. Se alguém comparar os
# MAGIC dois sem saber disso, compara fotografias tiradas em épocas diferentes.
# MAGIC
# MAGIC E repare na proporção: poucas das empresas nacionais têm código ICAO. O cadastro é dominado por
# MAGIC aviação agrícola, táxi aéreo e aeroclube, que não têm código de três letras. Quem voa linha
# MAGIC regular tem. Isso volta na gold, quando o join com o VRA for medido.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Códigos de operação — seed table, e ela **não** é bronze
# MAGIC
# MAGIC O VRA guarda `codigo_di = "0"` e `codigo_tipo_linha = "N"`. Sem tradução, isso não significa nada
# MAGIC para um analista — e significa menos ainda para um LLM.
# MAGIC
# MAGIC A ANAC publica essas descrições numa **página HTML**, não num CSV. Então esta tabela é uma
# MAGIC *seed table*: dado de referência pequeno, estável, curado à mão, versionado junto com o código.
# MAGIC É categoria legítima de fonte — o erro seria deixar esse mapeamento espalhado em `CASE WHEN`
# MAGIC dentro das queries.
# MAGIC
# MAGIC **Mas ela não pertence ao bronze.** Bronze é dado ingerido *como chegou*, com `_arquivo_origem` e
# MAGIC `_ingerido_em`. Esta tabela não tem arquivo de origem e não foi ingerida — foi digitada. Guardá-la
# MAGIC no bronze faz a camada descrever errado o que ela é. Vai para o schema `ref`, com `_curado_em` e
# MAGIC `_fonte_url` no lugar das colunas de ingestão.
# MAGIC
# MAGIC E um detalhe que a versão curta esquece: **`codigo_di = '1'` aparece em 6.338 voos e não consta
# MAGIC na tabela oficial.** Deixar de fora é fazer 6.338 linhas caírem num `NULL` silencioso. Declaramos
# MAGIC explicitamente como não catalogado — e 97,6% deles não têm horário previsto, ou seja, comportam-se
# MAGIC como etapa não programada.

# COMMAND ----------

FONTE = "https://www.gov.br/anac/pt-br/assuntos/dados-e-estatisticas/historico-de-voos"

CODIGOS = [
    # dominio,            codigo, descricao,                                            catalogado
    ("codigo_di",         "0", "Etapa Regular",                                            True),
    ("codigo_di",         "1", "NAO CATALOGADO - comporta-se como etapa nao programada",   False),
    ("codigo_di",         "2", "Etapa Extra",                                              True),
    ("codigo_di",         "3", "Etapa de Retorno",                                         True),
    ("codigo_di",         "4", "Inclusao de Etapa",                                        True),
    ("codigo_di",         "6", "Etapa Nao Remunerada Sem Transporte de Objetos",           True),
    ("codigo_di",         "7", "Etapa de Voo de Fretamento",                               True),
    ("codigo_di",         "9", "Etapa de Voo Charter",                                     True),
    ("codigo_di",         "D", "Etapa de Voo Duplicada",                                   True),
    ("codigo_di",         "E", "Etapa Nao Remunerada Com Transporte de Objetos",           True),
    ("codigo_tipo_linha", "N", "Domestica Mista",                                          True),
    ("codigo_tipo_linha", "C", "Domestica Cargueira",                                      True),
    ("codigo_tipo_linha", "I", "Internacional Mista",                                      True),
    ("codigo_tipo_linha", "G", "Internacional Cargueira",                                  True),
]

(spark.createDataFrame(CODIGOS, "dominio string, codigo string, descricao string, catalogado boolean")
 .withColumn("_fonte_url", F.lit(FONTE))
 .withColumn("_curado_em", F.current_timestamp())
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOGO}.ref.codigos_operacao"))

print(f"ref.codigos_operacao: {spark.table(f'{CATALOGO}.ref.codigos_operacao').count()} linhas")
display(spark.table(f"{CATALOGO}.ref.codigos_operacao").orderBy("dominio", "codigo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. O que o Delta guardou sem a gente pedir
# MAGIC
# MAGIC Ninguém escreveu uma linha de código de versionamento. Mesmo assim:

# COMMAND ----------

display(spark.sql(f"""
    SELECT version, timestamp, operation, operationMetrics.numOutputRows AS linhas_escritas
    FROM (DESCRIBE HISTORY {CATALOGO}.bronze.vra) ORDER BY version
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Rode a ingestão duas vezes e compare: mesmo número de linhas, `_ingerido_em` diferente.
# MAGIC É a prova de idempotência, reconstruída **do histórico**, sem ter guardado nada.
# MAGIC
# MAGIC Isso é propriedade do formato de tabela aberto, não código nosso: todo `write` no Delta grava um
# MAGIC commit no log de transações, e o dado antigo continua nos arquivos Parquet até um `VACUUM`.
# MAGIC Auditoria e rollback saem de graça.

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


# MAGIC ## 8. Comentários e fechamento

# COMMAND ----------

spark.sql(f"""
    COMMENT ON TABLE {CATALOGO}.bronze.vra IS
    'Bronze - VRA (Voo Regular Ativo) da ANAC, 12 meses. Dado bruto: todas as colunas string, nenhuma
     linha descartada, contagem reconciliada contra a contagem fisica de linhas dos arquivos.
     Traz _fonte_atualizada_em (data de publicacao declarada pela ANAC no cabecalho) e _marcador_registro
     (assinatura do sistema de origem, preservada antes do cast). Carga full refresh idempotente.'
""")

for coluna, comentario in {
    "_arquivo_origem":      "Auditoria: nome do arquivo CSV mensal de onde a linha veio.",
    "_ingerido_em":         "Auditoria: momento em que ESTA leitura aconteceu.",
    "_fonte_atualizada_em": "Data em que a ANAC declarou ter atualizado o arquivo, lida da primeira linha do CSV. Responde se o dado esta velho - _ingerido_em nao responde isso.",
    "_marcador_registro":   "Verdadeiro quando o horario previsto veio com fracao de segundo no arquivo. E a assinatura do sistema de malha internacional: 90 por cento dessas linhas sao voos internacionais. Linhagem, nao regra de negocio.",
}.items():
    spark.sql(f"ALTER TABLE {CATALOGO}.bronze.vra ALTER COLUMN {coluna} COMMENT '{sql_literal(comentario)}'")

COMENTARIOS_TABELA = {
    f"{CATALOGO}.bronze.aerodromos":
        "Bronze - cadastro de aerodromos PUBLICOS da ANAC, como chegou, com TODAS as colunas do arquivo. "
        "Chave: codigo ICAO (OACI). Traz latitude e longitude em grau decimal (LATGEOPOINT/LONGEOPOINT) e "
        "capacidade de operacao diurna e noturna - colunas que o recorte usual descarta. "
        "LIMITACAO CONHECIDA: cobre apenas aerodromos publicos. 16 aeroportos brasileiros do VRA "
        "(2.983 pontas de voo) estao no cadastro de aerodromos PRIVADOS, que nao faz parte deste projeto.",
    f"{CATALOGO}.bronze.empresas_nacionais":
        "Bronze - cadastro de empresas aereas NACIONAIS da ANAC, como chegou. Chave: codigo ICAO, vazio para "
        "operadores sem codigo (aviacao agricola, taxi aereo, aeroclube). Nao unir com empresas_estrangeiras "
        "nesta camada. 5 linhas deste arquivo tem aspas nao escapadas - ver bronze.referencias_malformadas.",
    f"{CATALOGO}.bronze.empresas_estrangeiras":
        "Bronze - cadastro de empresas aereas ESTRANGEIRAS autorizadas a operar no Brasil, como chegou. "
        "Cadastro separado do nacional na origem, mantido separado no bronze. Publicado em data diferente "
        "do cadastro nacional: conferir _fonte_atualizada_em antes de comparar os dois.",
    f"{CATALOGO}.bronze.referencias_malformadas":
        "Bronze - linhas dos arquivos de referencia cujo numero de campos difere do esperado, detectadas por "
        "contagem no texto cru (sem passar pelo parser de CSV). Existe porque o modo PERMISSIVE aceita linha "
        "torta em silencio: sem esta tabela, a corrupcao e invisivel.",
    f"{CATALOGO}.ref.codigos_operacao":
        "Referencia - seed table curada a mao a partir da descricao de variaveis da ANAC. Traduz codigo_di e "
        "codigo_tipo_linha. Fora do bronze de proposito: nao tem arquivo de origem nem ingestao, tem curadoria. "
        "A coluna catalogado marca codigo que aparece no dado e nao consta na tabela oficial.",
}

for tabela, comentario in COMENTARIOS_TABELA.items():
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{sql_literal(comentario)}'")

print("comentarios aplicados")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOGO}.bronze"))
display(spark.sql(f"SHOW TABLES IN {CATALOGO}.ref"))

# COMMAND ----------

display(spark.sql(f"""
    SELECT _arquivo_origem,
           COUNT(*)                  AS linhas,
           MAX(_fonte_atualizada_em) AS anac_publicou_em,
           MAX(_ingerido_em)         AS ingerido_em,
           SUM(CASE WHEN _marcador_registro THEN 1 ELSE 0 END) AS com_marcador
    FROM {CATALOGO}.bronze.vra
    GROUP BY _arquivo_origem ORDER BY _arquivo_origem
"""))
