import argparse
import glob
import sys
import unicodedata
import re
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql.window import Window

# ==========================================
# UTILITÁRIOS
# ==========================================

def _list_paths_with_glob(spark: SparkSession, glob_path: str) -> List[str]:
    try:
        jvm = spark._jvm
        sc = spark.sparkContext
        hconf = sc._jsc.hadoopConfiguration()
        Path = jvm.org.apache.hadoop.fs.Path
        path = Path(glob_path)
        fs = path.getFileSystem(hconf)
        statuses = fs.globStatus(path)
        return [status.getPath().toString() for status in statuses] if statuses else []
    except Exception:
        return [glob_path]

def get_jdbc_df(spark, table, opts):
    """Lê uma tabela do Postgres como DataFrame Spark."""
    return spark.read.format("jdbc").options(**opts).option("dbtable", table).load()

# ==========================================
# TRANSFORMAÇÃO E LIMPEZA
# ==========================================

def transform_sim_raw(df):
    """
    Limpeza inicial e tipagem dos dados brutos do SIM.
    Prepara os campos para os Joins com as dimensões.
    """
    # 1. Datas
    df = df.withColumn("data_obito", F.to_date(F.col("DTOBITO").cast("string"), "ddMMyyyy"))
    df = df.withColumn("data_nascimento", F.to_date(F.col("DTNASC").cast("string"), "ddMMyyyy"))

    # 2. Hora (HH:MM:SS)
    df = df.withColumn("hora_clean", F.lpad(F.col("HORAOBITO").cast("string"), 4, "0"))
    df = df.withColumn("tempo_obito", 
        F.when(
            (F.col("hora_clean").isNull()) | (F.col("hora_clean") > "2359"), 
            F.lit("00:00:00") # Default para ignorado ou inválido
        ).otherwise(
            F.concat(
                F.substring(F.col("hora_clean"), 1, 2), F.lit(":"),
                F.substring(F.col("hora_clean"), 3, 2), F.lit(":00")
            )
        )
    )

    # 3. Mapeamentos Demográficos (Códigos -> Descrições das Dimensões)
    
    # Sexo
    df = df.withColumn("sexo_desc", 
        F.when(F.col("SEXO") == "1", "Masculino")
         .when(F.col("SEXO") == "2", "Feminino")
         .otherwise("Ignorado")
    )
    
    # Raça/Cor
    df = df.withColumn("raca_desc", 
        F.when(F.col("RACACOR") == "1", "Branca")
         .when(F.col("RACACOR") == "2", "Preta")
         .when(F.col("RACACOR") == "3", "Amarela")
         .when(F.col("RACACOR") == "4", "Parda")
         .when(F.col("RACACOR") == "5", "Indígena")
         .otherwise("Ignorado") # Dicionário não especifica outros, assume ignorado
    )

    # Estado Civil
    df = df.withColumn("estciv_desc",
        F.when(F.col("ESTCIV") == "1", "Solteiro")
         .when(F.col("ESTCIV") == "2", "Casado")
         .when(F.col("ESTCIV") == "3", "Viúvo")
         .when(F.col("ESTCIV") == "4", "Separado judicialmente/divorciado")
         .when(F.col("ESTCIV") == "5", "União estável")
         .otherwise("Ignorado")
    )

    # Escolaridade
    # 1: Nenhuma, 2: 1 a 3 anos, 3: 4 a 7 anos, 4: 8 a 11 anos, 5: 12 e mais, 9: Ignorado
    df = df.withColumn("esc_desc",
        F.when(F.col("ESC") == "1", "Nenhuma")
         .when(F.col("ESC") == "2", "1 a 3 anos")
         .when(F.col("ESC") == "3", "4 a 7 anos")
         .when(F.col("ESC") == "4", "8 a 11 anos")
         .when(F.col("ESC") == "5", "12 e mais")
         .otherwise("Ignorado")
    )

    # Idade Calculada (Anos)
    # IDADE: 1 digito unidade + 2 digitos valor. 4=anos, 5=>100 anos.
    df = df.withColumn("id_unid", F.substring(F.lpad(F.col("IDADE"), 3, "0"), 1, 1).cast("int"))
    df = df.withColumn("id_val", F.substring(F.lpad(F.col("IDADE"), 3, "0"), 2, 2).cast("int"))
    
    df = df.withColumn("idade_anos",
        F.when(F.col("id_unid") < 4, 0)
         .when(F.col("id_unid") == 4, F.col("id_val"))
         .when(F.col("id_unid") == 5, F.col("id_val") + 100)
         .otherwise(None)
    )

    # Prepara CIDs para a Ponte (Limpeza básica)
    # LINHAA a LINHAD e CAUSABAS são códigos únicos
    for c in ["LINHAA", "LINHAB", "LINHAC", "LINHAD"]:
        df = df.withColumn(c, F.regexp_replace(F.col(c), "[^A-Z0-9]", ""))
        df = df.withColumn(c, F.regexp_replace(F.col(c), "X$", ""))

    # LINHAII pode conter múltiplos CIDs separados por * (ex: *I48X*N40X)
    # 1. Manter apenas alfanuméricos e asteriscos
    df = df.withColumn("LINHAII_clean", F.regexp_replace(F.col("LINHAII"), "[^A-Z0-9*]", ""))
    # 2. Splitar por *
    df = df.withColumn("LINHAII_arr", F.split(F.col("LINHAII_clean"), "\\*"))
    # 3. Remover strings vazias do array e remover sufixo X de cada elemento
    df = df.withColumn("LINHAII_arr", F.expr("filter(LINHAII_arr, x -> x != '')"))
    df = df.withColumn("LINHAII_arr", F.expr("transform(LINHAII_arr, x -> regexp_replace(x, 'X$', ''))"))

    # Conversão de Códigos para Join
    # Tratamento especial para municípios: valores vazios/nulos devem resultar em null, não em 0
    df = df.withColumn("cod_mun_res", 
        F.when((F.col("CODMUNRES").isNull()) | (F.trim(F.col("CODMUNRES")) == ""), None)
         .otherwise(F.col("CODMUNRES").cast("int"))
    )
    df = df.withColumn("cod_mun_ocor", 
        F.when((F.col("CODMUNOCOR").isNull()) | (F.trim(F.col("CODMUNOCOR")) == ""), None)
         .otherwise(F.col("CODMUNOCOR").cast("int"))
    )
    df = df.withColumn("ocupacao_cbo", F.trim(F.col("OCUP")))

    return df

# ==========================================
# LÓGICA DE NEGÓCIO PRINCIPAL
# ==========================================

def process_bridge_table(df_sim, df_dim_causa, jdbc_opts, spark):
    """
    Gerencia a criação e recuperação de IDs para grupos de causas.
    Retorna o DataFrame SIM enriquecido com 'chave_grupo_causa'.
    """
    print("Processando Grupos de Causas...")
    
    # 1. Recuperar a tabela ponte atual do banco
    df_bridge_db = get_jdbc_df(spark, "ponteGrupoCausas", jdbc_opts)
    
    # Criar um hash/assinatura para os grupos existentes
    # Agrupa por chave_grupo e coleta lista ordenada de (ordem, chave_causa)
    # Simplificação: Vamos assumir que a combinação de CIDs define o grupo.
    
    # 2. Transformar as colunas de CID do SIM em Array de CIDs
    # Mapear CIDs (strings) para Chaves (inteiros) usando dimCausa
    # Precisamos fazer lookup para LINHAA, LINHAB, etc.
    
    # Estratégia: Criar array unificado [A, B, C, D, II_1, II_2...] preservando posições
    # IMPORTANTE: Não concatenar diretamente porque isso perde as posições originais quando há nulls
    # Em vez disso, criar um array com estrutura (posição_original, código) para cada linha
    
    # Garantir que LINHAII_arr não seja null
    df_arrays = df_sim.select("row_id", "LINHAA", "LINHAB", "LINHAC", "LINHAD", "LINHAII_arr") \
        .withColumn("LINHAII_arr", F.coalesce(F.col("LINHAII_arr"), F.array()))
    
    # Criar arrays com estrutura (ordem, código) para cada linha
    # LINHAA=ordem 1, LINHAB=ordem 2, LINHAC=ordem 3, LINHAD=ordem 4
    # LINHAII elementos começam em ordem 5
    df_arrays = df_arrays.withColumn(
        "all_causes",
        F.concat(
            F.array(F.struct(F.lit(1).alias("ordem"), F.col("LINHAA").alias("codigo"))),
            F.array(F.struct(F.lit(2).alias("ordem"), F.col("LINHAB").alias("codigo"))),
            F.array(F.struct(F.lit(3).alias("ordem"), F.col("LINHAC").alias("codigo"))),
            F.array(F.struct(F.lit(4).alias("ordem"), F.col("LINHAD").alias("codigo"))),
            F.expr("transform(LINHAII_arr, (x, i) -> struct(cast(i + 5 as int) as ordem, x as codigo))")
        )
    )

    # Explodir e filtrar apenas códigos não nulos/não vazios
    df_causes_stacked = df_arrays.select("row_id", F.explode("all_causes").alias("causa_info")) \
        .select(
            "row_id", 
            F.col("causa_info.ordem").alias("ordem_causa"),
            F.col("causa_info.codigo").alias("cid_codigo")
        ).filter((F.col("cid_codigo").isNotNull()) & (F.col("cid_codigo") != ""))
    
    # Debug: mostrar exemplo de como as causas estão sendo processadas (sem ordenação para evitar uso de memória)
    print("Exemplo de causas processadas (primeiros 20 registros):")
    df_causes_stacked.show(20, truncate=False)

    # Join com dimCausa para pegar chave_causa
    # Usar LEFT JOIN para não perder a posição (ordem) se o CID não for encontrado
    # Se não achar, assume chave_causa = 0 (Ignorado/Não Encontrado)
    df_causes_mapped = df_causes_stacked.join(
        df_dim_causa.select(F.col("codigo_CID").alias("cid_codigo"), "chave_causa"),
        on="cid_codigo",
        how="left"
    ).fillna(0, subset=["chave_causa"])

    # 3. Criar Assinatura do Grupo para cada Óbito (row_id)
    # Assinatura: string concatenada "chave_causa:ordem|..."
    # IMPORTANTE: Preservar a ordem das causas (A, B, C, D, II)
    # A ordem indica a sequência de eventos que levaram ao óbito
    
    # Usar array_sort com struct para ordenar por ordem_causa antes de coletar
    # Reparticionar por row_id antes da agregação para distribuir melhor
    df_causes_mapped = df_causes_mapped.repartition(4, "row_id")
    
    df_groups = df_causes_mapped.withColumn(
        "item_sig", F.concat_ws(":", F.col("chave_causa"), F.col("ordem_causa"))
    ).withColumn(
        "causa_struct", F.struct("chave_causa", "ordem_causa")
    ).groupBy("row_id").agg(
        F.array_sort(F.collect_list(F.struct(F.col("ordem_causa"), F.col("item_sig")))).alias("sig_sorted"),
        F.array_sort(F.collect_list(F.struct(F.col("ordem_causa"), F.col("causa_struct")))).alias("causes_sorted")
    ).select(
        "row_id",
        F.concat_ws("|", F.expr("transform(sig_sorted, x -> x.item_sig)")).alias("group_signature"),
        F.expr("transform(causes_sorted, x -> x.causa_struct)").alias("causes_list")
    )
    
    # Reparticionar após a agregação para distribuir os grupos
    df_groups = df_groups.repartition(4)

    # 4. Ler Grupos Existentes e Criar suas Assinaturas
    # Se a tabela estiver vazia, cria DF vazio
    # Otimização: usar count() ao invés de isEmpty() que é mais eficiente
    bridge_count = df_bridge_db.count()
    print(f"Grupos de causas existentes no banco: {bridge_count}")
    
    if bridge_count == 0:
        df_existing_signatures = spark.createDataFrame([], schema=StructType([
            StructField("existing_group_id", IntegerType()), 
            StructField("group_signature", StringType())
        ]))
        next_id = 1
    else:
        # Recriar assinatura dos dados do banco para comparação
        # Ordenar por ordem_causa para manter a sequência correta
        df_existing_signatures = df_bridge_db.withColumn(
            "item_sig", F.concat_ws(":", F.col("chave_causa"), F.col("ordem_causa"))
        ).groupBy("chave_grupo_causa").agg(
            F.array_sort(F.collect_list(F.struct(F.col("ordem_causa"), F.col("item_sig")))).alias("sig_sorted")
        ).select(
            F.col("chave_grupo_causa").alias("existing_group_id"),
            F.concat_ws("|", F.expr("transform(sig_sorted, x -> x.item_sig)")).alias("group_signature")
        )
        
        # Calcular próximo ID
        max_id_row = df_bridge_db.agg(F.max("chave_grupo_causa")).collect()[0][0]
        next_id = (max_id_row + 1) if max_id_row else 1

    # 5. Cruzar Óbitos com Grupos Existentes
    df_merged = df_groups.join(df_existing_signatures, on="group_signature", how="left")

    # Separar Novos Grupos
    df_new_groups = df_merged.filter(F.col("existing_group_id").isNull()).select("group_signature", "causes_list").distinct()

    # Se houver novos grupos, atribuir IDs
    if not df_new_groups.rdd.isEmpty():
        w_new = Window.orderBy("group_signature") # Ordem determinística
        df_new_groups_ids = df_new_groups.withColumn("row_num", F.row_number().over(w_new)) \
            .withColumn("new_group_id", F.col("row_num") + F.lit(next_id - 1))
        
        # Preparar dados para inserir na ponteGrupoCausas
        # Explodir a lista de volta para linhas
        df_to_insert = df_new_groups_ids.select(
            F.col("new_group_id").alias("chave_grupo_causa"),
            F.explode("causes_list").alias("cause_struct")
        ).select(
            "chave_grupo_causa",
            F.col("cause_struct.chave_causa").alias("chave_causa"),
            F.col("cause_struct.ordem_causa").cast("int").alias("ordem_causa")
        )

        print(f"Inserindo {df_new_groups_ids.count()} novos grupos de causas na ponte.")
        df_to_insert.write.format("jdbc").options(**jdbc_opts).option("dbtable", "ponteGrupoCausas").mode("append").save()

        # Atualizar o mapeamento local para incluir os novos
        df_mapping_final = df_merged.join(
            df_new_groups_ids.select("group_signature", "new_group_id"),
            on="group_signature",
            how="left"
        ).withColumn("final_group_id", F.coalesce(F.col("existing_group_id"), F.col("new_group_id")))
    else:
        df_mapping_final = df_merged.withColumn("final_group_id", F.col("existing_group_id"))

    # Retornar DF original com a chave anexada
    return df_sim.join(
        df_mapping_final.select("row_id", "final_group_id"),
        on="row_id",
        how="left"
    ).withColumnRenamed("final_group_id", "chave_grupo_causa")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True) # Esperado 'sim'
    p.add_argument("--date", required=True)
    p.add_argument("--bucket", default="landing")
    p.add_argument("--prefix", default="source_sus")
    p.add_argument("--pg-url", default="jdbc:postgresql://postgres-olap:5432/olap_db")
    p.add_argument("--pg-user", default="olap")
    p.add_argument("--pg-password", default="olap")
    p.add_argument("--table-prefix", default="")
    args = p.parse_args()

    spark = SparkSession.builder.appName("ETL_SIM_FactObitos").getOrCreate()

    # 1. Listar arquivos CSV
    if args.bucket == "local":
        base_path = f"{args.prefix}/{args.dataset}/dt={args.date}/*.csv"
        files = glob.glob(base_path)
    else:
        base_path = f"s3a://{args.bucket}/{args.prefix}/{args.dataset}/dt={args.date}/*.csv"
        files = _list_paths_with_glob(spark, base_path)

    if not files:
        print(f"Sem arquivos em {base_path}.")
        return

    print(f"Encontrados {len(files)} arquivo(s) para processar")
    for idx, file in enumerate(files, 1):
        print(f"  [{idx}] {file}")

    # 2. Ler Dimensões (uma vez, antes do loop)
    jdbc_opts = {
        "url": args.pg_url, "user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"
    }
    
    # Ler dimensões e fazer broadcast das menores para otimizar joins
    # NÃO fazer cache de tudo - apenas broadcast das pequenas
    dim_data = F.broadcast(get_jdbc_df(spark, "dimData", jdbc_opts))
    
    # Ler dimHorario e formatar tempo para string HH:MM:SS para garantir join correto
    dim_horario = F.broadcast(
        get_jdbc_df(spark, "dimHorario", jdbc_opts) \
        .withColumn("tempo_str", F.format_string("%02d:%02d:%02d", F.col("hora"), F.col("minutos"), F.col("segundos")))
    )

    dim_municipio = F.broadcast(get_jdbc_df(spark, "dimMunicipio", jdbc_opts))
    # Criar coluna de 6 dígitos para join com CSV que usa 6 dígitos
    dim_municipio = dim_municipio.withColumn("cod_mun_6", F.floor(F.col("codigo_ibge") / 10).cast("int"))

    dim_ocupacao = F.broadcast(get_jdbc_df(spark, "dimOcupacao", jdbc_opts))
    dim_demografia = F.broadcast(get_jdbc_df(spark, "dimDemografia", jdbc_opts))
    dim_causa = F.broadcast(get_jdbc_df(spark, "dimCausa", jdbc_opts))

    # 3. Processar cada arquivo individualmente
    print("\n" + "="*60)
    print("INICIANDO PROCESSAMENTO DOS ARQUIVOS")
    print("="*60 + "\n")
    
    for file_idx, file_path in enumerate(files, 1):
        print(f"\n{'='*60}")
        print(f"Processando arquivo {file_idx}/{len(files)}: {file_path}")
        print(f"{'='*60}\n")
        
        # 3.1. Ler arquivo individual
        df_raw = spark.read.option("header", "true").option("sep", ";").option("inferSchema", "false").csv(file_path)
        record_count = df_raw.count()
        print(f"Registros no arquivo: {record_count}")
        
        if record_count == 0:
            print(f"Arquivo vazio, pulando...")
            continue
        
        # Adicionar ID único temporário
        df_raw = df_raw.withColumn("row_id", F.monotonically_increasing_id())
        
        # 3.2. Limpeza Inicial
        df_clean = transform_sim_raw(df_raw)
        df_clean = df_clean.repartition(2)  # Menos partições para arquivos individuais
        
        # 3.3. Processar Grupos de Causas (Ponte)
        df_with_causes = process_bridge_table(df_clean, dim_causa, jdbc_opts, spark)
        
        # 3.4. Lookups para outras Chaves (Joins)
        
        # Data Nascimento
        df_joined = df_with_causes.join(
            dim_data.select(F.col("data").alias("data_nascimento"), F.col("chave_data").alias("chave_data_nascimento")),
            on="data_nascimento", how="left"
        )
        
        # Data Óbito
        df_joined = df_joined.join(
            dim_data.select(F.col("data").alias("data_obito"), F.col("chave_data").alias("chave_data_obito")),
            on="data_obito", how="left"
        )

        # Tempo/Hora
        df_joined = df_joined.join(
            dim_horario.select("tempo_str", "chave_tempo"),
            df_joined.tempo_obito == F.col("tempo_str"),
            how="left"
        ).withColumnRenamed("chave_tempo", "chave_tempo_obito").drop("tempo_str")

        # Municípios (Residencia e Ocorrencia)
        dim_mun_res = dim_municipio.select(
            F.col("cod_mun_6").alias("cod_mun_res"), 
            F.col("chave_municipio").alias("chave_municipio_residencia")
        )
        dim_mun_ocor = dim_municipio.select(
            F.col("cod_mun_6").alias("cod_mun_ocor"), 
            F.col("chave_municipio").alias("chave_municipio_obito")
        )
        
        df_joined = df_joined.join(dim_mun_res, on="cod_mun_res", how="left")
        df_joined = df_joined.join(dim_mun_ocor, on="cod_mun_ocor", how="left")
        
        # Debug: verificar quantos municípios não fizeram match
        total_records = df_joined.count()
        null_mun_res = df_joined.filter(F.col("chave_municipio_residencia").isNull() & F.col("cod_mun_res").isNotNull()).count()
        null_mun_ocor = df_joined.filter(F.col("chave_municipio_obito").isNull() & F.col("cod_mun_ocor").isNotNull()).count()
        empty_mun_ocor = df_joined.filter(F.col("cod_mun_ocor").isNull()).count()
        
        print(f"Total de registros: {total_records}")
        print(f"Municípios de residência não encontrados: {null_mun_res}")
        print(f"Municípios de ocorrência não encontrados: {null_mun_ocor}")
        print(f"Registros com código de município de ocorrência vazio/null: {empty_mun_ocor}")

        # Ocupação
        df_joined = df_joined.join(
            dim_ocupacao.select(F.col("cbo_2002").alias("ocupacao_cbo"), "chave_ocupacao"),
            on="ocupacao_cbo", how="left"
        )

        # Demografia
        df_joined = df_joined.join(
            dim_demografia,
            (df_joined.sexo_desc == dim_demografia.descricao_sexo) &
            (df_joined.raca_desc == dim_demografia.raca) &
            (df_joined.estciv_desc == dim_demografia.estado_civil) &
            (df_joined.esc_desc == dim_demografia.escolaridade) &
            (df_joined.idade_anos >= dim_demografia.idade_minima) &
            (
                 dim_demografia.idade_maxima.isNull() | 
                (df_joined.idade_anos <= dim_demografia.idade_maxima)
            ),
            how="left"
        )
        
        # 3.5. Agregação Final
        keys = [
            "chave_data_nascimento",
            "chave_data_obito",
            "chave_tempo_obito",
            "chave_municipio_residencia",
            "chave_municipio_obito",
            "chave_demografia",
            "chave_grupo_causa",
            "chave_ocupacao"
        ]
        
        # Tratar NULLs antes de agrupar/inserir
        # Município de residência: preencher com 0 se join falhou MAS código existia
        df_joined = df_joined.withColumn(
            "chave_municipio_residencia",
            F.when(F.col("chave_municipio_residencia").isNull() & F.col("cod_mun_res").isNotNull(), 0)
             .otherwise(F.col("chave_municipio_residencia"))
        )
        
        # Município de ocorrência: preencher com 0 se join falhou MAS código existia
        df_joined = df_joined.withColumn(
            "chave_municipio_obito",
            F.when(F.col("chave_municipio_obito").isNull() & F.col("cod_mun_ocor").isNotNull(), 0)
             .otherwise(F.col("chave_municipio_obito"))
        )
        
        # Ocupação e grupo de causa: sempre preencher com 0 se null
        df_joined = df_joined.fillna(0, subset=["chave_ocupacao", "chave_grupo_causa"])
        
        # Reparticionar antes da agregação
        df_joined = df_joined.repartition(4, *keys)
        
        df_agg = df_joined.groupBy(*keys).count().withColumnRenamed("count", "quantidade_obitos")
        
        # Filtra nulos restantes para evitar crash do job
        df_agg = df_agg.na.drop(subset=keys)
        
        # 3.5. Escrita na Fato (append)
        agg_count = df_agg.count()
        print(f"\nEscrevendo {agg_count} registros agregados na factObitos...")
        df_agg.write.format("jdbc").options(**jdbc_opts).option("dbtable", "factObitos").mode("append").save()
        
        print(f"✓ Arquivo {file_idx}/{len(files)} processado com sucesso!\n")
        
        # Liberar memória
        df_raw.unpersist()
        df_clean.unpersist()
    
    print("\n" + "="*60)
    print("PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
    print("="*60 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()