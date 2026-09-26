-- =====================================================================
--  VoeBem — Fase 2: verificacao da carga
-- =====================================================================
--  Rode DEPOIS do 01_carga_voebem_PRONTO.sql.
--
--  Contar linha nao prova nada: as duas armadilhas da carga (booleano
--  virando zero, nulo virando zero) mantem a contagem intacta e destroem
--  o conteudo. Estes testes olham o conteudo.
--
--  Cada bloco imprime o valor obtido e o esperado, lado a lado.
-- =====================================================================

USE voebem;


-- ---------------------------------------------------------------------
-- TESTE 1 — Contagem de linhas
-- ---------------------------------------------------------------------
SELECT 'dim_aeroporto'      AS tabela, COUNT(*) AS linhas,   396 AS esperado FROM dim_aeroporto
UNION ALL SELECT 'dim_empresa',        COUNT(*),             118 FROM dim_empresa
UNION ALL SELECT 'kpi_diario',         COUNT(*),          21571 FROM kpi_diario
UNION ALL SELECT 'kpi_rota_mensal',    COUNT(*),          12525 FROM kpi_rota_mensal
UNION ALL SELECT 'kpi_aeroporto_hora', COUNT(*),          10021 FROM kpi_aeroporto_hora
UNION ALL SELECT 'voos_fantasma',      COUNT(*),             54 FROM voos_fantasma
UNION ALL SELECT 'dias_atipicos',      COUNT(*),            365 FROM dias_atipicos;


-- ---------------------------------------------------------------------
-- TESTE 2 — Os booleanos sobreviveram? (armadilha 1)
-- ---------------------------------------------------------------------
-- Se alguma linha "verdadeiros" vier ZERO, o 'true' virou 0 na carga.
SELECT 'dias_atipicos.dia_atipico'   AS flag,
       SUM(dia_atipico)              AS verdadeiros,
       '3 (30/09, 10/12, 11/12)'     AS esperado
FROM dias_atipicos
UNION ALL
SELECT 'dim_aeroporto.no_cadastro_anac', SUM(no_cadastro_anac), '162 de 396'
FROM dim_aeroporto
UNION ALL
SELECT 'dim_aeroporto.opera_a_noite',    SUM(opera_a_noite),    'entre 1 e 162, nunca 0'
FROM dim_aeroporto
UNION ALL
SELECT 'dim_empresa.no_cadastro_anac',   SUM(no_cadastro_anac), '111 de 118'
FROM dim_empresa;


-- ---------------------------------------------------------------------
-- TESTE 3 — Os nulos sobreviveram? (armadilha 2)
-- ---------------------------------------------------------------------
-- 'zerados_suspeitos' TEM QUE SER 0. Se for maior, virou zero quem
-- deveria ser nulo — e no mapa esses aeroportos vao parar no Golfo da Guine.
SELECT
  SUM(latitude IS NULL)                             AS sem_coordenada_nulo,
  SUM(latitude = 0 AND longitude = 0)               AS zerados_suspeitos,
  SUM(utc_offset IS NULL)                           AS sem_fuso_nulo,
  '~234 nulos, 0 zerados'                           AS esperado
FROM dim_aeroporto;


-- ---------------------------------------------------------------------
-- TESTE 4 — Acentuacao (encoding)
-- ---------------------------------------------------------------------
-- Tem que sair "Placido de Castro" com A de verdade, nao "Pl?cido".
SELECT icao_aeroporto, nome_aeroporto, municipio_aeroporto, uf_aeroporto
FROM dim_aeroporto
WHERE icao_aeroporto IN ('SBRB','SBGR','SBSP','SBBE','SBFZ')
ORDER BY icao_aeroporto;


-- ---------------------------------------------------------------------
-- TESTE 5 — Datas viraram data mesmo?
-- ---------------------------------------------------------------------
SELECT MIN(dia) AS primeiro_dia, MAX(dia) AS ultimo_dia, COUNT(*) AS dias
FROM dias_atipicos;
-- esperado: 2025-08-01 a 2026-07-31 (aprox), 365 dias

SELECT MIN(mes_referencia) AS primeiro_mes, MAX(mes_referencia) AS ultimo_mes
FROM kpi_rota_mensal;


-- =====================================================================
--  AS QUERIES DE ANALISE
--  Cada uma reproduz um achado do projeto. Sao elas que o dashboard
--  vai desenhar — vale conferir aqui antes de virar gráfico.
-- =====================================================================


-- ---------------------------------------------------------------------
-- A. EFEITO CASCATA — a pergunta "a que horas vale voar?"
-- ---------------------------------------------------------------------
-- Vira a pagina 2 do dashboard. Esperado: sobe de 8,0% as 5h para 25,9% as 22h (25,7% as 23h).
-- O denominador so conta os grupos com valor: os 306 grupos com pontualidade
-- nula nao podem entrar como se fossem 0% pontuais.
SELECT
  hora_brasilia                                                   AS hora,
  SUM(voos)                                                       AS voos,
  ROUND(SUM(voos * pontualidade_pct)
        / SUM(CASE WHEN pontualidade_pct IS NOT NULL THEN voos END), 1)       AS pontualidade_pct,
  ROUND(100 - SUM(voos * pontualidade_pct)
        / SUM(CASE WHEN pontualidade_pct IS NOT NULL THEN voos END), 1)       AS atraso_pct,
  ROUND(SUM(voos * atraso_partida_medio)
        / SUM(CASE WHEN atraso_partida_medio IS NOT NULL THEN voos END), 1)   AS atraso_medio_min
FROM kpi_aeroporto_hora
GROUP BY hora_brasilia
ORDER BY hora_brasilia;
-- NOTA: pontualidade e uma media PONDERADA por voos. Somar percentual
-- direto (AVG da coluna) daria peso igual a um aeroporto de 50 mil voos
-- e a um de 100. E o erro mais comum em dashboard.


-- ---------------------------------------------------------------------
-- B. RANKING HONESTO — o botao liga/desliga dos voos-fantasma
-- ---------------------------------------------------------------------
-- Vira a pagina 3. Esperado: Iberia cai de ~54% para ~6%.
SELECT
  e.nome_companhia,
  SUM(k.voos)                                                     AS registros,
  SUM(k.linhas_fantasma)                                          AS linhas_fantasma,
  ROUND(100 * SUM(k.cancelados + k.linhas_fantasma) / SUM(k.voos), 2) AS cancelamento_bruto,
  ROUND(100 * SUM(k.cancelados) / NULLIF(SUM(k.voos - k.linhas_fantasma), 0), 2) AS cancelamento_real,
  ROUND(100 * SUM(k.partidas_pontuais) / NULLIF(SUM(k.partidas_avaliaveis), 0), 2) AS pontualidade_pct
FROM kpi_diario k
JOIN dim_empresa e ON e.icao_empresa = k.icao_empresa
GROUP BY e.nome_companhia
HAVING SUM(k.voos) >= 2000
ORDER BY cancelamento_bruto DESC;


-- ---------------------------------------------------------------------
-- C. INDICE DE CONFIABILIDADE — a rota e confiavel, ou so na media?
-- ---------------------------------------------------------------------
-- Vira a pagina 4. Repare em atraso_medio x atraso_p90.
SELECT
  r.rota_icao,
  e.nome_companhia,
  SUM(r.voos_previstos)                       AS voos,
  MAX(r.km_rota)                              AS km,
  MAX(r.faixa_etapa)                          AS faixa,
  ROUND(AVG(r.atraso_medio), 1)               AS atraso_medio,
  ROUND(AVG(r.atraso_p90), 1)                 AS atraso_p90,
  ROUND(AVG(r.atraso_p90) - AVG(r.atraso_medio), 1) AS quanto_a_media_esconde,
  ROUND(AVG(r.indice_confiabilidade), 1)      AS indice
FROM kpi_rota_mensal r
JOIN dim_empresa e ON e.icao_empresa = r.icao_empresa
GROUP BY r.rota_icao, e.nome_companhia
HAVING SUM(r.voos_previstos) >= 1000
ORDER BY quanto_a_media_esconde DESC
LIMIT 20;


-- ---------------------------------------------------------------------
-- D. EXEMPLO: POR QUE O p90 IMPORTA
-- ---------------------------------------------------------------------
-- Rotas com atraso medio quase igual e p90 completamente diferente.
-- Esperado (media dos p90 mensais, a mesma conta do dashboard): SBNF->SBGR
-- (TAM) com p90 ~41,4 vs SBGR->SBGO (GOL) com ~26,7, as duas com media ~7 min.
-- (Com o p90 calculado sobre o periodo inteiro, da 40,0 e 26,0.)
SELECT
  rota_icao, icao_empresa,
  SUM(voos_previstos)           AS voos,
  ROUND(AVG(atraso_medio), 1)   AS atraso_medio,
  ROUND(AVG(atraso_p90), 1)     AS atraso_p90
FROM kpi_rota_mensal
GROUP BY rota_icao, icao_empresa
HAVING SUM(voos_previstos) >= 1000 AND AVG(atraso_medio) BETWEEN 6.5 AND 8.5
ORDER BY atraso_p90 DESC;


-- ---------------------------------------------------------------------
-- E. DIA ATIPICO — o que e evento e o que e desempenho
-- ---------------------------------------------------------------------
SELECT dia, voos, cancelados, taxa_pct, mediana_periodo_pct,
       vezes_a_mediana, dia_atipico
FROM dias_atipicos
ORDER BY taxa_pct DESC
LIMIT 10;

-- e o efeito de excluir esses dias do indicador nacional:
SELECT
  ROUND(100 * SUM(cancelados) / SUM(voos), 2)                          AS cancelamento_geral,
  ROUND(100 * SUM(IF(dia_atipico, 0, cancelados))
            / NULLIF(SUM(IF(dia_atipico, 0, voos)), 0), 2)             AS so_em_dia_normal
FROM dias_atipicos;


-- ---------------------------------------------------------------------
-- F. MAPA — aeroportos com coordenada e pontualidade
-- ---------------------------------------------------------------------
-- Vira a pagina 5. Se algum aparecer com latitude 0, a armadilha 2 pegou voce.
SELECT
  a.icao_aeroporto, a.nome_aeroporto, a.praca_aeroporto, a.uf_aeroporto,
  a.latitude, a.longitude, a.utc_offset, a.opera_a_noite,
  SUM(k.voos)                                                        AS voos,
  ROUND(SUM(k.voos * k.pontualidade_pct) / SUM(k.voos), 1)           AS pontualidade_pct
FROM kpi_aeroporto_hora k
JOIN dim_aeroporto a ON a.icao_aeroporto = k.icao_aeroporto
WHERE a.latitude IS NOT NULL
GROUP BY a.icao_aeroporto, a.nome_aeroporto, a.praca_aeroporto, a.uf_aeroporto,
         a.latitude, a.longitude, a.utc_offset, a.opera_a_noite
HAVING SUM(k.voos) >= 500
ORDER BY voos DESC;


-- ---------------------------------------------------------------------
-- G. O FUSO NA PRATICA — hora de Brasilia x hora local
-- ---------------------------------------------------------------------
-- A regra R3 visivel numa query: mesmo aeroporto, dois relogios.
SELECT
  a.icao_aeroporto, a.nome_aeroporto, a.utc_offset,
  SUM(IF(k.hora_brasilia     BETWEEN 0 AND 4, k.voos, 0)) AS voos_00_04_brasilia,
  SUM(IF(k.hora_local_origem BETWEEN 0 AND 4, k.voos, 0)) AS voos_00_04_local,
  SUM(k.voos)                                             AS total
FROM kpi_aeroporto_hora k
JOIN dim_aeroporto a ON a.icao_aeroporto = k.icao_aeroporto
WHERE a.utc_offset IS NOT NULL
GROUP BY a.icao_aeroporto, a.nome_aeroporto, a.utc_offset
HAVING SUM(k.voos) >= 1000
ORDER BY a.utc_offset, total DESC;
