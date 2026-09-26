CREATE DATABASE IF NOT EXISTS voebem CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE voebem;

DROP TABLE IF EXISTS `dim_aeroporto`;
CREATE TABLE `dim_aeroporto` (
  `icao_aeroporto` VARCHAR(255),
  `nome_aeroporto` VARCHAR(255),
  `municipio_aeroporto` VARCHAR(255),
  `praca_aeroporto` VARCHAR(255),
  `uf_aeroporto` VARCHAR(255),
  `latitude` DOUBLE,
  `longitude` DOUBLE,
  `altitude_m` DOUBLE,
  `operacao_noturna` VARCHAR(255),
  `opera_a_noite` TINYINT(1),
  `pais_aeroporto` VARCHAR(255),
  `utc_offset` INT,
  `no_cadastro_anac` TINYINT(1),
  PRIMARY KEY (icao_aeroporto)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `dim_empresa`;
CREATE TABLE `dim_empresa` (
  `icao_empresa` VARCHAR(255),
  `nome_companhia` VARCHAR(255),
  `sigla_iata` VARCHAR(255),
  `servico_autorizado` VARCHAR(255),
  `cadastro_companhia` VARCHAR(255),
  `no_cadastro_anac` TINYINT(1),
  PRIMARY KEY (icao_empresa)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `kpi_diario`;
CREATE TABLE `kpi_diario` (
  `dia` DATE,
  `icao_empresa` VARCHAR(255),
  `escopo_voo` VARCHAR(255),
  `dia_atipico` TINYINT(1),
  `voos` BIGINT,
  `realizados` BIGINT,
  `cancelados` BIGINT,
  `linhas_fantasma` BIGINT,
  `partidas_pontuais` BIGINT,
  `partidas_avaliaveis` BIGINT,
  `chegadas_pontuais` BIGINT,
  `chegadas_avaliaveis` BIGINT,
  `atraso_chegada_medio` DOUBLE,
  KEY idx_dia (dia), KEY idx_emp (icao_empresa)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `kpi_rota_mensal`;
CREATE TABLE `kpi_rota_mensal` (
  `rota_icao` VARCHAR(255),
  `icao_origem` VARCHAR(255),
  `icao_destino` VARCHAR(255),
  `icao_empresa` VARCHAR(255),
  `mes_referencia` DATETIME,
  `escopo_voo` VARCHAR(255),
  `faixa_etapa` VARCHAR(255),
  `km_rota` DOUBLE,
  `voos_previstos` BIGINT,
  `voos_realizados` BIGINT,
  `cancelamentos` BIGINT,
  `pontualidade_chegada` DECIMAL(15,4),
  `taxa_cancelamento` DECIMAL(15,4),
  `atraso_medio` DOUBLE,
  `atraso_p50` INT,
  `atraso_p90` INT,
  `indice_confiabilidade` DECIMAL(20,1),
  KEY idx_rota (rota_icao), KEY idx_mes (mes_referencia)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `kpi_aeroporto_hora`;
CREATE TABLE `kpi_aeroporto_hora` (
  `icao_aeroporto` VARCHAR(255),
  `hora_brasilia` INT,
  `hora_local_origem` INT,
  `dia_semana` VARCHAR(255),
  `num_dia_semana` INT,
  `voos` BIGINT,
  `pontualidade_pct` DECIMAL(17,2),
  `atraso_partida_medio` DOUBLE,
  `atraso_partida_p90` INT,
  KEY idx_apt (icao_aeroporto), KEY idx_hora (hora_brasilia)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `voos_fantasma`;
CREATE TABLE `voos_fantasma` (
  `icao_empresa` VARCHAR(255),
  `numero_voo` VARCHAR(255),
  `ocorrencias` BIGINT,
  `cancelados` BIGINT,
  `taxa_cancelamento` DECIMAL(27,2),
  `com_partida_real` BIGINT,
  KEY idx_emp (icao_empresa)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `dias_atipicos`;
CREATE TABLE `dias_atipicos` (
  `dia` DATE,
  `voos` BIGINT,
  `cancelados` BIGINT,
  `taxa_pct` DOUBLE,
  `mediana_periodo_pct` DOUBLE,
  `vezes_a_mediana` DOUBLE,
  `dia_atipico` TINYINT(1),
  PRIMARY KEY (dia)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
