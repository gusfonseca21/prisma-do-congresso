#!/bin/sh

# Script para facilitar o processo de recriação do schema do banco de dados. Irá excluir e recriar o schema, passar os privilégios para o usuário do banco de dados da pipeline, excluir os arquivos de versionamento da estrutura das tabelas do Alembic e recriá-las no novo schema.

PROJECT_ROOT="$(cd "$(dirname "$0")/../../../" && pwd)"

nome_usuario_pipeline_banco=prisma_parlamentar_pipeline
nome_banco=prisma_parlamentar

echo "Tem certeza que deseja resetar o banco de dados? Isso irá apagar e recriar o schema e apagar os arquivos de versionamento do Alembic. [s/n]"
read resposta
if [ "$resposta" = "s" ]; then

	DATABASE_URL=$(cat "$PROJECT_ROOT/.env" | grep DATABASE_OWNER_URL | cut -d '=' -f2-)
	DATABASE_URL="$(printf '%s' "$DATABASE_URL" | sed 's/search_path=/search_path%3D/')"

	echo "Destruindo, recriando o schema e garantindo permissões ao usuário da aplicação ao banco de dados."

	psql "$DATABASE_URL" <<EOF
drop schema if exists oltp cascade;
create schema if not exists oltp;
grant all privileges on schema oltp to $nome_usuario_pipeline_banco
EOF

	echo "Deseja excluir arquivos de versionamento do banco de dados do Alembic? [s/n]"
	read resposta_versionamento
	if [ "$resposta_versionamento" = "s" ]; then
		rm -f "$PROJECT_ROOT/pipeline/src/database/migrations/versions/"*.py
		if [ $? -eq 0 ]; then
		    echo "Arquivos de versionamento excluídos com sucesso."
		else
		    echo "Erro ao excluir arquivos de versionamento."
		fi
	else
		echo "Os arquivos de versionamento não serão excluídos."
	fi

	echo "Recriando a estrutura do banco de dados."
	cd "$PROJECT_ROOT"/pipeline && uv run alembic revision --autogenerate && uv run alembic upgrade head
	if [ $? -eq 0 ]; then
		    echo "Migrações aplicadas com sucesso."
		else
		    echo "Erro ao executar as migrações."
	fi


else
	echo "O banco de dados NÃO será resetado. Saindo do programa."
fi
