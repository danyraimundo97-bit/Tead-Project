# Usa a imagem oficial mais recente
FROM apache/superset:latest

# Muda para root para ter permissões de instalação
USER root

# Ativa o ambiente virtual do Superset e instala o driver do Trino usando o "uv" (recomendado pela Apache)
RUN . /app/.venv/bin/activate && \
    uv pip install trino[sqlalchemy]

# Volta para o utilizador de segurança do Superset
USER superset