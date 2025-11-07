from pathlib import Path
from uuid import UUID
import httpx, json, asyncio, hashlib, zipfile, os, shutil
from typing import Any, cast
from prefect import get_run_logger
from prefect.exceptions import MissingContextError
from prefect.artifacts import update_progress_artifact, aupdate_progress_artifact
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from config.loader import load_config

APP_SETTINGS = load_config()

def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

# Armazena em memória ou grava em disco uma lista de JSONs
async def fetch_json_many_async(
        urls: list[str],
        out_dir: str | Path | None = None,
        concurrency: int = 10,
        timeout: float = 30.0,
        follow_pagination: bool = True,
        logger: Any | None = None,
        progress_artifact_id: Any | None = None,
) -> list[str] | list[dict]:
    """
    - Se out_dir for fornecido, salva cada JSON em um arquivo e retorna a lista de caminhos
    - Caso contrário, retorna a lista de dicionários em memória
    """

    def log(msg: str):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    sem = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=max(concurrency, 10))
    timeout_cfg = httpx.Timeout(timeout)

    ensure_dir(out_dir) if out_dir else None
    
    queue = asyncio.Queue()
    processed_urls = set() # Evita processar a mesma URL duas vezes
    results = []

    for u in urls:
        queue.put_nowait(u)

    downloaded_urls = 0
    update_lock = asyncio.Lock() # Evita race conditions ao atualizar o progresso de forma assíncrona

    async with httpx.AsyncClient(limits=limits, timeout=timeout_cfg) as client:
        async def worker():
            nonlocal downloaded_urls

            while True:
                try:
                    u = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                
                if u in processed_urls:
                    continue
                processed_urls.add(u)

                async with sem:
                    log(f"Baixando: {u}")
                    r = await client.get(u)
                    r.raise_for_status()
                    data = r.json()

                    # Salva ou retorna em memória
                    if out_dir:
                        name = hashlib.sha1(u.encode()).hexdigest() + ".json"
                        path = Path(out_dir) / name
                        with open(path, "w", encoding="utf-8") as f:
                            json.dump(data, f, ensure_ascii=False)
                        results.append(str(path))
                    else:
                        results.append(data)

                    # Atualiza progresso
                    if progress_artifact_id:
                        async with update_lock:
                            downloaded_urls += 1
                            await aupdate_progress_artifact(
                                artifact_id=progress_artifact_id,
                                progress=(downloaded_urls / max(len(urls),1)) * 100
                            )

                # Se tiver paginação, adiciona novas URLs à fila
                if follow_pagination and "links" in data:
                    links = {l["rel"]: l["href"] for l in data["links"]}
                    if "self" in links and "last" in links:
                        for extra in generate_pages_urls(links["self"], links["last"]):
                            if extra not in processed_urls:
                                queue.put_nowait(extra)

        # Inicia os workers
        workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
        await asyncio.gather(*workers)

        valid_results = [
            r for r in results
            if not isinstance(r, BaseException)
        ]

        print(valid_results)

        return valid_results


def generate_pages_urls(url_self: str, url_last: str):
    """
    Caso a url baixada tenha mais páginas, retorna uma lista com as páginas adicionais a serem baixadas
    """
    self_parsed = urlparse(url_self)
    self_params = parse_qs(self_parsed.query)
    self_page = int(self_params.get("pagina", ["1"])[0])

    # Se não for a primeira página, retorna pois todas as URLs já foram geradas
    if self_page > 1:
        return []

    # Pega o número da última página
    last_parsed = urlparse(url_last)
    last_params = parse_qs(last_parsed.query)
    last_page = int(last_params.get("pagina", ["1"])[0])

    urls = []
    for page in range(2, (last_page + 1)):
        params = parse_qs(self_parsed.query)
        params["pagina"] = [str(page)]
        new_query = urlencode(params, doseq=True)
        new_url = urlunparse(self_parsed._replace(query=new_query))
        urls.append(new_url)

    return urls

if __name__ == "__main__":
    urls = ["https://dadosabertos.camara.leg.br/api/v2/deputados/152605/discursos?dataInicio=2023-01-01&ordenarPor=dataHoraInicio&ordem=DESC&itens=100&pagina=1"]
    asyncio.run(fetch_json_many_async(urls))














    # Armazena em memória ou grava em disco uma lista de JSONs
# async def fetch_json_many_async(
#         urls: list[str],
#         out_dir: str | Path | None = None,
#         concurrency: int = 10,
#         timeout: float = 30.0,
#         follow_pagination: bool = True,
#         logger: Any | None = None,
#         progress_artifact_id: Any | None = None,
# ) -> list[str] | list[dict]:
#     """
#     - Se out_dir for fornecido, salva cada JSON em um arquivo e retorna a lista de caminhos
#     - Caso contrário, retorna a lista de dicionários em memória
#     """
#     logger = logger or _get_prefect_logger_or_none()

#     def log(msg: str):
#         if logger:
#             logger.info(msg)
#         else:
#             print(msg)

#     sem = asyncio.Semaphore(concurrency)
#     limits = httpx.Limits(max_connections=max(concurrency, 10))
#     timeout_cfg = httpx.Timeout(timeout)

#     ensure_dir(out_dir) if out_dir else None
    
#     queue = asyncio.Queue()
#     processed_urls = set() # Evita processar a mesma URL duas vezes
#     results = []

#     for u in urls:
#         queue.put_nowait(u)

#     downloaded_urls = 0
#     update_lock = asyncio.Lock() # Evita race conditions ao atualizar o progresso de forma assíncrona

#     async def one(u: str, client: httpx.AsyncClient):
#         nonlocal downloaded_urls

#         if u in processed_urls:
#             return []
#         processed_urls.add(u)

#         async with sem:
#             log(f"Fazendo download da URL: {u}")
#             r = await client.get(u)
#             r.raise_for_status()
#             data = r.json()

#             # Salvar ou retornar o resultado atual
#             if out_dir:
#                 # Nome do arquivo determinado pelo Hash da URL
#                 name = hashlib.sha1(u.encode()).hexdigest() + ".json"
#                 path = Path(out_dir) / name
#                 with open(path, "w", encoding="utf-8") as f:
#                     json.dump(data, f, ensure_ascii=False)
#                 current_result = str(path)
#             else:
#                 current_result = data

#             if progress_artifact_id and len(urls) > 0:
#                 async with update_lock:
#                     downloaded_urls += 1
#                     await aupdate_progress_artifact(
#                         artifact_id=progress_artifact_id,
#                         progress=(downloaded_urls / len(urls)) * 100
#                     )

#         # Verifica se deve serguir a paginação
#         additional_pages_urls = []
#         if follow_pagination and "links" in data:
#             links = {link["rel"]: link["href"] for link in data["links"]}
#             if "self" in links and "last" in links:
#                 for additional_page
                
    
#     async with httpx.AsyncClient(limits=limits, timeout=timeout_cfg) as client:
#         tasks = [one(u, client) for u in urls]
#         nested_results = await asyncio.gather(*tasks, return_exceptions=APP_SETTINGS.FLOW.TASKS_RETURN_EXCEPTION)

#     # Achata a lista de resultados
#     for item in nested_results:
#         if isinstance(item, list):
#             results.extend(item)
#         else:
#             results.append(item)

#     # Elimina os resultados inválidos (erro)
#     valid_results = [
#         result for result in results
#         if not isinstance(result, BaseException)
#     ]

#     return valid_results