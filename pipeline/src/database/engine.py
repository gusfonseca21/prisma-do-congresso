import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

_engine = None


def get_engine(database_url: str = os.getenv("DATABASE_URL", "")):
    """
    Retorna o engine singleton. Na primeira chamada, cria o engine. Nas chamadas seguintes, retorna a instância já existente.
    """
    global _engine

    # Cuidado com a defnição do tamanho da pool do banco de dados. O limite do banco no plano Essential-0 do Heroku é de 20 conexões simultâneas.

    if _engine is None:
        _engine = create_engine(
            url=database_url,
            echo=False,
            echo_pool=True,
            pool_size=5,
            max_overflow=5,
            pool_timeout=30,
        )
        return _engine

    return _engine


@contextmanager
def get_connection():
    """
    Gerenciador de contexto que fornece uma conexão do pool.
    Realiza transações atômicas por padrão.
    A conexão é sempre devolvida ao pool ao final.
    """
    engine = get_engine()
    if engine is None:
        raise ValueError("Não existe uma Engine aberta para utilizar uma Conexão.")
    with engine.begin() as conn:
        yield conn
