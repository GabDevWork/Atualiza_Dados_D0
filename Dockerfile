# Base da imagem DBT Snowflake [cite: 1]
FROM ghcr.io/dbt-labs/dbt-snowflake:1.8.latest

# Diretório de trabalho
WORKDIR /usr/datamart

# Variáveis de ambiente para DBT
ENV \
  DBT_PROFILES_DIR=/usr/datamart/.dbt/profiles \
  DBT_MODULES_DIR=/usr/datamart/.dbt/modules

# Copiar arquivos do DBT e credenciais ofuscadas
COPY ./<ARQUIVO_PROFILES>.yml ${DBT_PROFILES_DIR}/profiles.yml
COPY ./ ./

# Garantir permissão de administrador para instalar dependências no Debian
USER root

# Atualizar e instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Instalar AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install [cite: 2] \
    && rm -rf awscliv2.zip ./aws

# Instalar pacotes do Python necessários (pyyaml mantido)
RUN pip install --upgrade pip && \
    pip install pandas sqlalchemy pymysql snowflake-snowpark-python snowflake-connector-python[pandas] google-api-python-client google-auth-httplib2 google-auth-oauthlib pyyaml

# Garantir permissões de execução nos scripts Python
RUN find /usr/datamart -maxdepth 1 -name "*.py" -exec chmod +x {} \;

# Comando padrão (Sem Entrypoint atrapalhando) [cite: 3]
CMD ["dbt", "run"]