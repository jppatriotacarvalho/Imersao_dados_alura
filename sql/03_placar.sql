-- =====================================================================
--  VoeBem — placar da carga: TODOS os testes numa tabela so
-- =====================================================================
--  Roda esta unica query e olha a coluna "status".
--  Se tudo der PASSA, a Fase 2 esta fechada.
-- =====================================================================

USE voebem;

SELECT
  teste,
  obtido,
  esperado,
  IF(ok, 'PASSA', '>>>>> FALHA <<<<<') AS status
FROM (
  -- ---------- contagem de linhas ----------
  SELECT 1 AS ord, 'linhas . dim_aeroporto'       AS teste, COUNT(*) AS obtido, '396'   AS esperado, COUNT(*) = 396   AS ok FROM dim_aeroporto
  UNION ALL
  SELECT 2, 'linhas . dim_empresa',        COUNT(*), '118',   COUNT(*) = 118   FROM dim_empresa
  UNION ALL
  SELECT 3, 'linhas . kpi_diario',         COUNT(*), '21571', COUNT(*) = 21571 FROM kpi_diario
  UNION ALL
  SELECT 4, 'linhas . kpi_rota_mensal',    COUNT(*), '12525', COUNT(*) = 12525 FROM kpi_rota_mensal
  UNION ALL
  SELECT 5, 'linhas . kpi_aeroporto_hora', COUNT(*), '10021', COUNT(*) = 10021 FROM kpi_aeroporto_hora
  UNION ALL
  SELECT 6, 'linhas . voos_fantasma',      COUNT(*), '54',    COUNT(*) = 54    FROM voos_fantasma
  UNION ALL
  SELECT 7, 'linhas . dias_atipicos',      COUNT(*), '365',   COUNT(*) = 365   FROM dias_atipicos

  -- ---------- ARMADILHA 1: booleano virou zero? ----------
  UNION ALL
  SELECT 10, 'booleano . dias atipicos marcados',
         SUM(dia_atipico), '3', SUM(dia_atipico) = 3 FROM dias_atipicos
  UNION ALL
  SELECT 11, 'booleano . aeroportos no cadastro ANAC',
         SUM(no_cadastro_anac), '162', SUM(no_cadastro_anac) = 162 FROM dim_aeroporto
  UNION ALL
  SELECT 12, 'booleano . aeroportos que operam a noite',
         SUM(opera_a_noite), 'maior que 0', SUM(opera_a_noite) > 0 FROM dim_aeroporto
  UNION ALL
  SELECT 13, 'booleano . empresas no cadastro ANAC',
         SUM(no_cadastro_anac), '111', SUM(no_cadastro_anac) = 111 FROM dim_empresa

  -- ---------- ARMADILHA 2: nulo virou zero? ----------
  UNION ALL
  SELECT 20, 'nulo . aeroportos sem coordenada (estrangeiros)',
         SUM(latitude IS NULL), '234', SUM(latitude IS NULL) = 234 FROM dim_aeroporto
  UNION ALL
  SELECT 21, 'nulo . coordenada 0,0 = Golfo da Guine',
         SUM(COALESCE(latitude = 0 AND longitude = 0, 0)), '0',
         SUM(COALESCE(latitude = 0 AND longitude = 0, 0)) = 0 FROM dim_aeroporto
  UNION ALL
  SELECT 22, 'nulo . aeroportos sem fuso (estrangeiros)',
         SUM(utc_offset IS NULL), '234', SUM(utc_offset IS NULL) = 234 FROM dim_aeroporto

  -- ---------- encoding ----------
  UNION ALL
  SELECT 30, 'acentuacao . nomes com caractere quebrado',
         SUM(nome_aeroporto LIKE '%?%'), '0',
         SUM(nome_aeroporto LIKE '%?%') = 0 FROM dim_aeroporto

  -- ---------- os achados do projeto, agora como assercao ----------
  UNION ALL
  SELECT 40, 'achado . Congonhas entre 00h e 04h (toque de recolher)',
         COALESCE(SUM(voos), 0), '0', COALESCE(SUM(voos), 0) = 0
  FROM kpi_aeroporto_hora WHERE icao_aeroporto = 'SBSP' AND hora_brasilia BETWEEN 0 AND 4
  UNION ALL
  SELECT 41, 'achado . voos-fantasma com 98 por cento ou mais cancelados',
         SUM(taxa_cancelamento >= 98), '54', SUM(taxa_cancelamento >= 98) = 54 FROM voos_fantasma
  UNION ALL
  SELECT 42, 'achado . dia pior que 15 por cento de cancelamento',
         SUM(taxa_pct > 15), '1 (10/12/2025)', SUM(taxa_pct > 15) = 1 FROM dias_atipicos
) t
ORDER BY ord;
