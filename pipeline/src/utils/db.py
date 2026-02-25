from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import Insert
from sqlalchemy.sql.elements import ColumnElement


def columns_to_compare(table: sa.Table, external_id: str) -> list[sa.Column[Any]]:
    """
    Retorna os nomes de todas as colunas para comparação.
    Serve para, no conflito, durante inserção de dados, atualizar todos os campos.
    Não deve incluir no retorno o id externo da tabela, por exemplo id_deputado.
    """
    return [c for c in table.c if c.name != external_id]


def update_dict(
    stmt: Insert, columns_to_compare: list[sa.Column[Any]]
) -> dict[str, Any]:
    return {c.name: getattr(stmt.excluded, c.name) for c in columns_to_compare}


def where_clause(
    table: sa.Table, stmt: Insert, columns_to_compare: list[sa.Column[Any]]
) -> ColumnElement[bool]:
    return sa.or_(
        *[
            getattr(table.c, c.name).is_distinct_from(getattr(stmt.excluded, c.name))
            for c in columns_to_compare
        ]
    )
