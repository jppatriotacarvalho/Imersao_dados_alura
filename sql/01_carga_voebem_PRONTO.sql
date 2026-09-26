-- =====================================================================
--  VoeBem — Fase 2 & 4: carga dos agregados da gold no MySQL (Docker)
-- =====================================================================
--  Ordem de inicialização automática no contêiner (só na 1ª vez que o banco nasce):
--    1) 01_schema.sql (cria o database e as 7 tabelas)
--    2) 02_carga.sql  (ESTE arquivo — carrega os 7 CSVs de /var/lib/mysql-files)
--  O placar (sql/03_placar.sql, 18 testes) NÃO roda sozinho: rode à mão:
--    docker compose exec mysql sh -c 'mysql -uroot -p$MYSQL_ROOT_PASSWORD -t voebem < /sql/03_placar.sql'
-- =====================================================================

-- ---------------------------------------------------------------------
-- CARGA NO CONTÊINER (Docker)
-- ---------------------------------------------------------------------
-- A carga roda dentro do contêiner MySQL lendo os CSVs montados em
-- /var/lib/mysql-files (diretório seguro configurado via secure_file_priv).
-- Sem a opção LOCAL, o MySQL opera em modo estrito (valores inválidos
-- causam erro imediato e interrompem a carga em vez de virar zero silencioso).

USE voebem;


-- ---------------------------------------------------------------------
-- AS DUAS ARMADILHAS DESTA CARGA
-- ---------------------------------------------------------------------
-- Elas nao dao erro. Elas corrompem o dado e seguem em frente.
--
-- ARMADILHA 1 — BOOLEANO VIRA ZERO
--   O Spark escreve booleano como o TEXTO 'true' / 'false'.
--   O MySQL recebe isso numa coluna TINYINT(1), nao entende, grava 0 e
--   emite apenas um WARNING. Resultado: TODAS as flags viram falso.
--   Voce descobriria isso no dashboard, quando 'dia_atipico' nunca
--   acontecesse e 'opera_a_noite' fosse falso para 396 aeroportos.
--   Correcao: ler para variavel e converter -> (@v = 'true')
--
-- ARMADILHA 2 — NULO VIRA ZERO
--   O export grava ausencia como campo VAZIO. Numa coluna DOUBLE, o
--   MySQL grava 0 (mais um warning ignorado).
--   Consequencia concreta: aeroporto estrangeiro nao tem latitude nem
--   longitude. Viraria (0,0) — que fica no Golfo da Guine. No mapa da
--   Fase 3, 234 aeroportos apareceriam boiando na Africa.
--   Correcao: ler para variavel e converter -> NULLIF(@v, '')
--
-- Por isso cada LOAD abaixo lista as colunas problematicas como @variavel
-- e converte no SET. E chato de escrever e e a diferenca entre o dado
-- certo e o dado errado.
-- ---------------------------------------------------------------------


-- =====================================================================
-- PARTE 1 — DIMENSOES
-- =====================================================================

-- dim_aeroporto: 396 linhas
-- booleanos: opera_a_noite, no_cadastro_anac
-- nulos    : latitude, longitude, altitude_m, utc_offset, opera_a_noite,
--            municipio, praca, uf (vazios para aeroporto estrangeiro)
LOAD DATA INFILE '/var/lib/mysql-files/dim_aeroporto.csv'
INTO TABLE dim_aeroporto
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(icao_aeroporto, nome_aeroporto, @municipio_aeroporto, @praca_aeroporto,
 @uf_aeroporto, @latitude, @longitude, @altitude_m, @operacao_noturna,
 @opera_a_noite, pais_aeroporto, @utc_offset, @no_cadastro_anac)
SET
  municipio_aeroporto = NULLIF(@municipio_aeroporto, ''),
  praca_aeroporto     = NULLIF(@praca_aeroporto, ''),
  uf_aeroporto        = NULLIF(@uf_aeroporto, ''),
  latitude            = NULLIF(@latitude, ''),
  longitude           = NULLIF(@longitude, ''),
  altitude_m          = NULLIF(@altitude_m, ''),
  operacao_noturna    = NULLIF(@operacao_noturna, ''),
  opera_a_noite       = IF(@opera_a_noite = '', NULL, @opera_a_noite = 'true'),
  utc_offset          = NULLIF(@utc_offset, ''),
  no_cadastro_anac    = (@no_cadastro_anac = 'true');


-- dim_empresa: 118 linhas
-- booleanos: no_cadastro_anac
LOAD DATA INFILE '/var/lib/mysql-files/dim_empresa.csv'
INTO TABLE dim_empresa
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(icao_empresa, nome_companhia, @sigla_iata, @servico_autorizado,
 @cadastro_companhia, @no_cadastro_anac)
SET
  sigla_iata         = NULLIF(@sigla_iata, ''),
  servico_autorizado = NULLIF(@servico_autorizado, ''),
  cadastro_companhia = NULLIF(@cadastro_companhia, ''),
  no_cadastro_anac   = (@no_cadastro_anac = 'true');


-- =====================================================================
-- PARTE 2 — AGREGADOS
-- =====================================================================

-- kpi_diario: 21.571 linhas
-- booleanos: dia_atipico
LOAD DATA INFILE '/var/lib/mysql-files/kpi_diario.csv'
INTO TABLE kpi_diario
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(dia, icao_empresa, @escopo_voo, @dia_atipico, voos, realizados, cancelados,
 linhas_fantasma, partidas_pontuais, partidas_avaliaveis,
 chegadas_pontuais, chegadas_avaliaveis, @atraso_chegada_medio)
SET
  escopo_voo           = NULLIF(@escopo_voo, ''),
  dia_atipico          = (@dia_atipico = 'true'),
  atraso_chegada_medio = NULLIF(@atraso_chegada_medio, '');


-- kpi_rota_mensal: 12.525 linhas  (a tabela do indice de confiabilidade)
LOAD DATA INFILE '/var/lib/mysql-files/kpi_rota_mensal.csv'
INTO TABLE kpi_rota_mensal
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(rota_icao, icao_origem, icao_destino, icao_empresa, mes_referencia,
 @escopo_voo, @faixa_etapa, @km_rota, voos_previstos, voos_realizados,
 cancelamentos, @pontualidade_chegada, @taxa_cancelamento, @atraso_medio,
 @atraso_p50, @atraso_p90, @indice_confiabilidade)
SET
  escopo_voo            = NULLIF(@escopo_voo, ''),
  faixa_etapa           = NULLIF(@faixa_etapa, ''),
  km_rota               = NULLIF(@km_rota, ''),
  pontualidade_chegada  = NULLIF(@pontualidade_chegada, ''),
  taxa_cancelamento     = NULLIF(@taxa_cancelamento, ''),
  atraso_medio          = NULLIF(@atraso_medio, ''),
  atraso_p50            = NULLIF(@atraso_p50, ''),
  atraso_p90            = NULLIF(@atraso_p90, ''),
  indice_confiabilidade = NULLIF(@indice_confiabilidade, '');


-- kpi_aeroporto_hora: 10.021 linhas  (base do grafico por hora e do mapa de calor)
-- hora_local_origem e NULA para aeroporto estrangeiro — de proposito
LOAD DATA INFILE '/var/lib/mysql-files/kpi_aeroporto_hora.csv'
INTO TABLE kpi_aeroporto_hora
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(icao_aeroporto, hora_brasilia, @hora_local_origem, @dia_semana,
 num_dia_semana, voos, @pontualidade_pct, @atraso_partida_medio,
 @atraso_partida_p90)
SET
  hora_local_origem    = NULLIF(@hora_local_origem, ''),
  dia_semana           = NULLIF(@dia_semana, ''),
  pontualidade_pct     = NULLIF(@pontualidade_pct, ''),
  atraso_partida_medio = NULLIF(@atraso_partida_medio, ''),
  atraso_partida_p90   = NULLIF(@atraso_partida_p90, '');


-- voos_fantasma: 54 linhas  (a regra R2)
LOAD DATA INFILE '/var/lib/mysql-files/voos_fantasma.csv'
INTO TABLE voos_fantasma
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(icao_empresa, numero_voo, ocorrencias, cancelados, taxa_cancelamento,
 com_partida_real);


-- dias_atipicos: 365 linhas  (a regra R5)
-- booleanos: dia_atipico
LOAD DATA INFILE '/var/lib/mysql-files/dias_atipicos.csv'
INTO TABLE dias_atipicos
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(dia, voos, cancelados, taxa_pct, mediana_periodo_pct, vezes_a_mediana,
 @dia_atipico)
SET
  dia_atipico = (@dia_atipico = 'true');


-- =====================================================================
-- PARTE 3 — Ver os avisos que o MySQL engoliu
-- =====================================================================
SHOW WARNINGS;
