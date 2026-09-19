
create database scoped credential cred_musk
WITH
   IDENTITY = 'Managed Identity'

create external data source source_silver
WITH(
    location = 'https://awstoragedatalakemusk.blob.core.windows.net/silver',
    CREDENTIAL = cred_musk
)

create external data source source_gold
WITH(
    location = 'https://awstoragedatalakemusk.blob.core.windows.net/gold',
    CREDENTIAL = cred_musk
)

create external file format format_parquet
WITH(
     format_type = parquet,
     DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'

)

---create external tables extslaes

create EXTERNAL table gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)AS
select *FROM gold.sales

select *from gold.extsales






