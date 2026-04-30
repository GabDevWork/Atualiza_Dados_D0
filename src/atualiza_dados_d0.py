import yaml
from snowflake.snowpark.session import Session

# ==============================================================================
# 1. CONEXÃO SNOWFLAKE (Padrão replicado)
# ==============================================================================
def conectar_snowflake():
    print("🔌 Carregando credenciais do arquivo YAML...")
    try:
        with open("<ARQUIVO_PROFILES>.yml", "r") as file:
            profile_data = yaml.safe_load(file)

        snowflake_params = profile_data["credentials"]["outputs"]["prod"]

        snowflake_config = {
            "account": snowflake_params.get("account", ""),
            "user": snowflake_params.get("user", ""),
            "password": snowflake_params.get("password", ""),
            "warehouse": snowflake_params.get("warehouse", ""),
            "database": snowflake_params.get("database", "<DATABASE>"),
            "schema": "<SCHEMA_BRONZE>",
            "role": snowflake_params.get("role", "<ROLE_NAME>"), 
            "client_session_keep_alive": True,
            "network_timeout": 300,
            "retry_attempts": 10
        }
        
        session = Session.builder.configs(snowflake_config).create()
        print("✅ Conexão Snowflake estabelecida com sucesso!")
        return session
    except Exception as e:
        print(f"❌ Erro ao conectar no Snowflake: {e}")
        raise

# ==============================================================================
# 2. MAIN - EXECUÇÃO DAS QUERIES SEQUENCIAIS
# ==============================================================================
def main():
    print("🚀 Iniciando atualização da tabela <TABLE_TARGET>...")
    session = None
    
    try:
        session = conectar_snowflake()

        # Dicionário/Lista com todas as tarefas SQL em ordem de execução
        queries = {
            "1. Limpa tabela": """
                DELETE FROM <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET>
                WHERE DATA_REF <> CURRENT_DATE();
            """,
            
            "2. Popula tabela": """
                INSERT INTO <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET> (
                    DTA_ATU, DATA_REF, ID_FILIAL, NOME_FILIAL, ID_EQUIPAMENTO, METRICA_A,
                    METRICA_B, HORA_PROG, HORA_CONF, HR_EFETIVA,
                    FORNECEDOR, COD_AREA, DES_AREA
                )
                SELECT 
                    CURRENT_TIMESTAMP() AS DTA_ATU, H.DATA_REFERENCIA, H.ID_FILIAL, H.NOME_FILIAL, H.ID_EQUIPAMENTO,
                    H.METRICA_A, H.METRICA_B, H.HORA_PROG,
                    H.HORA_CONF_1, HR_EFETIVA, H.FORNECEDOR, H.COD_AREA, H.DES_AREA 
                FROM ( 
                    SELECT DISTINCT  
                        A.DATA_REFERENCIA, A.ID_FILIAL, A.NOME_FILIAL, A.ID_EQUIPAMENTO, 
                        A.METRICA_A, A.METRICA_B, A.HORA_PROG, A.HORA_CONF, 
                        CAST(NULL AS TIMESTAMP_NTZ) AS HR_EFETIVA,
                        TO_TIMESTAMP_NTZ(CURRENT_DATE() || ' ' || LEFT(CAST(DATEADD(hour, -2, TRY_TO_TIME(MIN(A.HORA_CONF_1) || ':00')) AS VARCHAR), 5) || ':00') AS HORA_CONF_1,
                        B.QTDE_TOTAL, B.FORNECEDOR, B.COD_AREA, B.DES_AREA,
                        CASE WHEN H_T.ID_FILIAL IS NOT NULL THEN 'S' ELSE NULL END AS FLAG_ESPECIFICA,
                        CASE WHEN A.METRICA_A > 0 THEN 1 ELSE 0 END AS FLAG_ACAO_HOJE,
                        CASE WHEN A.SALDO_ATUAL <= 30000 THEN 1 ELSE 0 END AS FLAG_LIMITE_INFERIOR
                        -- Demais regras de negócio ofuscadas
                    FROM <DATABASE>.<SCHEMA_BRONZE>.<TABLE_DIM_PRINCIPAL> A
                    LEFT JOIN <DATABASE>.<SCHEMA_BRONZE>.<TABLE_ACOMPANHAMENTO> B ON A.ID_FILIAL = B.ID_FILIAL
                    LEFT JOIN <DATABASE>.<SCHEMA_BRONZE>.<TABLE_POSICAO> D ON A.ID_EQUIPAMENTO = D.ID_EQUIPAMENTO
                    LEFT JOIN <DATABASE>.<SCHEMA_BRONZE>.<TABLE_HORARIOS> H_T ON H_T.ID_FILIAL = A.ID_FILIAL
                    WHERE A.ID_FILIAL != <ID_EXCLUIDO> AND A.METRICA_A > 0
                    GROUP BY 
                        A.DATA_REFERENCIA, A.ID_FILIAL, A.NOME_FILIAL, A.ID_EQUIPAMENTO, A.SALDO_ATUAL, 
                        A.METRICA_A, A.METRICA_B, A.HORA_PROG, A.HORA_CONF, B.QTDE_TOTAL, B.FORNECEDOR, 
                        B.COD_AREA, B.DES_AREA, H_T.ID_FILIAL
                ) H
                WHERE NOT EXISTS(
                    SELECT 1 FROM <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET> T 
                    WHERE T.ID_FILIAL = H.ID_FILIAL AND T.ID_EQUIPAMENTO = H.ID_EQUIPAMENTO 
                    AND T.METRICA_A = H.METRICA_A 
                    AND T.HORA_PROG = H.HORA_PROG
                )
            """,

            "3. Deleta duplicatas": """
                BEGIN
                    CREATE OR REPLACE TRANSIENT TABLE <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET_TEMP> AS
                    SELECT * FROM <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET>
                    QUALIFY RANK() OVER(PARTITION BY ID_EQUIPAMENTO, HORA_PROG ORDER BY METRICA_B DESC) = 1;
                    
                    TRUNCATE TABLE <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET>;
                    
                    INSERT INTO <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET> 
                    SELECT * FROM <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET_TEMP>;
                END;
            """,

            "4. Update Hora Confirmada": """
                UPDATE <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET> AS A
                SET A.HORA_CONF = TO_TIMESTAMP_NTZ(CURRENT_DATE() || ' ' || LEFT(CAST(DATEADD(hour, -2, TRY_TO_TIME(B.HORA_CONF_1 || ':00')) AS VARCHAR), 5) || ':00')
                FROM <DATABASE>.<SCHEMA_BRONZE>.<TABLE_DIM_PRINCIPAL> AS B
                WHERE A.ID_EQUIPAMENTO = B.ID_EQUIPAMENTO 
                    AND A.HORA_PROG = B.HORA_PROG
                    AND A.HORA_CONF IS NULL;
            """,

            "5. Update Metrica Efetivada": """
                UPDATE <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET> AS A
                SET A.METRICA_B = B.METRICA_B
                FROM <DATABASE>.<SCHEMA_BRONZE>.<TABLE_DIM_PRINCIPAL> AS B
                WHERE A.ID_EQUIPAMENTO = B.ID_EQUIPAMENTO 
                    AND A.HORA_PROG = B.HORA_PROG
                    AND A.METRICA_B = 0;
            """,

            "6. Update HR EFETIVA": """
                UPDATE <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET>
                SET HR_EFETIVA = CURRENT_TIMESTAMP()
                WHERE METRICA_B > 0 AND HR_EFETIVA IS NULL;
            """,

            "7. Update DTA_ATU": """
                UPDATE <DATABASE>.<SCHEMA_BRONZE>.<TABLE_TARGET>
                SET DTA_ATU = CURRENT_TIMESTAMP();
            """
        }

        # Laço para executar cada query sequencialmente
        for nome_etapa, query in queries.items():
            print(f"⚙️ Executando: {nome_etapa}...")
            session.sql(query).collect()
            print(f"✔️ {nome_etapa} concluída!")

        print("🏁 Todos os processos rodaram com sucesso no banco!")

    except Exception as e:
        print(f"❌ Falha na execução: {e}")
        raise 
        
    finally:
        if session is not None:
            session.close()
            print("🔌 Sessão Snowflake encerrada.")

if __name__ == "__main__":
    main()