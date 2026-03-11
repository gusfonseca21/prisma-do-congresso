# MUITOS PROBLEMAS PARA AUTOMATIZAR A INICIALIZAÇÃO, PAUSAR POR ENQUANTO

# #!/bin/sh

# # Script de inicialização automatizada do pipeline para Dev

# PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"

# echo "Iniciando Pipeline"

# echo "Verificando se o servidor Prefect está ativo"
# server_alive=$(curl -sf http://127.0.0.1:4200/api/health)
# if [ "$server_alive" = "true" ]; then
# 	uv run prefect server stop
# else
# 	echo "O servidor Prefect está parado. Iniciando servidor Prefect."
# 	echo "$server_alive"
# 	uv run prefect server start --background

# 	uv run prefect server status --wait # Retorna quando o servidor estiver disponível

# 	echo "Defina parâmetro 'start_date' (vazio para valor padrão)."
# 	read start_date < /dev/tty

# 	echo "Defina parâmetro para 'end_date' (vazio para valor padrão)."
# 	read end_date < /dev/tty

# 	echo "Defina o parâmetro para 'use_files' ('y' para verdadeiro ou vazio para falso)."
# 	read use_files < /dev/tty

# 	params=""
# 	if [ -n "$start_date" ]; then
# 		params="$params -p start_date=$start_date"
# 	fi
# 	if [ -n "$end_date" ]; then
# 		params="$params -p end_date=$end_date"
# 	fi
# 	if [ "$use_files" = "y" ]; then
# 		params="$params -p use_files=true"
# 	fi

# 	uv run ./src/main.py

# 	uv run prefect deployment run 'Pipeline Flow/prisma-do-congresso-pipeline' $params
# fi
