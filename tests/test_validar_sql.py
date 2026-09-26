import pytest
from agente import validar_sql


def test_select_simples_recebe_limit():
    valido, sql = validar_sql("SELECT * FROM t")
    assert valido is True
    assert sql.strip().endswith("LIMIT 500")


def test_with_cte_valido():
    valido, sql = validar_sql("WITH a AS (SELECT 1) SELECT * FROM a")
    assert valido is True
    assert "LIMIT 500" in sql


def test_select_com_ponto_e_virgula_final():
    valido, sql = validar_sql("SELECT * FROM t;")
    assert valido is True
    assert not sql.endswith(";")
    assert sql.strip().endswith("LIMIT 500")


def test_rejeita_multiplas_instrucoes():
    valido, msg = validar_sql("SELECT 1; SELECT 2")
    assert valido is False
    assert "Múltiplas instruções" in msg


def test_rejeita_multiplas_instrucoes_com_drop():
    valido, msg = validar_sql("SELECT 1; DROP TABLE t")
    assert valido is False


def test_rejeita_ddl_drop():
    valido, msg = validar_sql("DROP TABLE t")
    assert valido is False
    assert "não é de leitura" in msg or "proibido" in msg


def test_rejeita_catalogo_sistema_e_information_schema():
    valido, msg = validar_sql("SELECT * FROM system.information_schema.tables")
    assert valido is False
    assert "information_schema" in msg or "sistema" in msg


def test_subquery_com_limit_ganha_limit_externo():
    valido, sql = validar_sql("SELECT * FROM (SELECT * FROM t LIMIT 5) x")
    assert valido is True
    assert sql.strip().endswith("LIMIT 500")


def test_limit_excessivo_reduz_para_500():
    valido, sql = validar_sql("SELECT * FROM t LIMIT 10000")
    assert valido is True
    assert sql.strip().endswith("LIMIT 500")
    assert "10000" not in sql


def test_permite_coluna_com_subpalavra_proibida():
    valido, sql = validar_sql("SELECT created_at FROM t")
    assert valido is True
    assert "created_at" in sql


def test_permite_funcao_replace():
    valido, sql = validar_sql("SELECT replace(nome, 'a', 'b') FROM t")
    assert valido is True
    assert "replace(" in sql


def test_remove_cercas_markdown():
    bloco = """```sql
SELECT * FROM t
```"""
    valido, sql = validar_sql(bloco)
    assert valido is True
    assert not sql.startswith("```")
    assert not sql.endswith("```")
    assert sql.strip().endswith("LIMIT 500")
