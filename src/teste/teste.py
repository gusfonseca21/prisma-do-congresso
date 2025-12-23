import asyncio

from utils.fetch_many_camara import fetch_many_camara


async def start():
    url = "https://dadosabertos.camara.leg.br/api/v2/deputados/152605/discursos?dataInicio=2024-01-01&dataFim=2024-12-31&ordenarPor=dataHoraInicio&ordem=DESC&itens=10"

    results = fetch_many_camara([url])


if __name__ == "__main__":
    asyncio.run(start())
