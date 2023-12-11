from pyspark.sql import SparkSession

# 以下自分の環境の値にセットする
CATALOG_NAME="catalog_iceberg"
SCHEMA_NAME="demodb"
DB_BUCKET="catalog_iceberg"
DB_BUCKET_ENDPOINT="s3.direct.us-south.cloud-object-storage.appdomain.cloud"
DB_BUCKET_ACCESS_KEY = "xxxxxxxxxxx"
DB_BUCKET_SECRET_KEY = "xxxxxxxxxxx"
SOURCE_BUCKET="spark-source"
SOURCE_BUCKET_ENDPOINT="s3.direct.us-south.cloud-object-storage.appdomain.cloud"
SOURCE_BUCKET_ACCESS_KEY = "xxxxxxxxxxx"
SOURCE_BUCKET_SECRET_KEY = "xxxxxxxxxxx"

def init_spark():
  spark = SparkSession.builder\
    .appName("lh-hms-cloud-nishito")\
    .config("spark.hadoop.fs.s3a.bucket."+DB_BUCKET+".endpoint" ,DB_BUCKET_ENDPOINT)\
    .config("spark.hadoop.fs.s3a.bucket."+DB_BUCKET+".access.key", DB_BUCKET_ACCESS_KEY)\
    .config("spark.hadoop.fs.s3a.bucket."+DB_BUCKET+".secret.key" ,DB_BUCKET_SECRET_KEY)\
    .config("spark.hadoop.fs.s3a.bucket."+SOURCE_BUCKET+".endpoint" ,SOURCE_BUCKET_ENDPOINT)\
    .config("spark.hadoop.fs.s3a.bucket."+SOURCE_BUCKET+".access.key" ,SOURCE_BUCKET_ACCESS_KEY)\
    .config("spark.hadoop.fs.s3a.bucket."+SOURCE_BUCKET+".secret.key" ,SOURCE_BUCKET_SECRET_KEY)\
    .config("spark.sql.warehouse.dir", f"s3://{DB_BUCKET}/")\
    .enableHiveSupport().getOrCreate()
  return spark

def create_database(spark):
    # Create a database in the lakehouse catalog: 
    # スキーマの作成
    spark.sql(f"create database if not exists {CATALOG_NAME}.{SCHEMA_NAME} LOCATION 's3a://{DB_BUCKET}/'")

def list_databases(spark):
    # list the database under lakehouse catalog
    # スキーマ一覧のリスト
    spark.sql(f"show databases from  {CATALOG_NAME}").show()

def basic_iceberg_table_operations(spark):
    # demonstration: Create a basic Iceberg table, insert some data and then query table
    # Icebergテーブルの作成、データのインサート、テーブルの照会
    spark.sql(f"create table if not exists  {CATALOG_NAME}.{SCHEMA_NAME}.testTable(id INTEGER, name VARCHAR(10), age INTEGER, salary DECIMAL(10, 2)) using iceberg").show()
    spark.sql(f"insert into  {CATALOG_NAME}.{SCHEMA_NAME}.testTable values(1,'Alan',23,3400.00),(2,'Ben',30,5500.00),(3,'Chen',35,6500.00)")
    spark.sql(f"select * from  {CATALOG_NAME}.{SCHEMA_NAME}.testTable").show()


def create_table_from_parquet_data(spark):
    # load parquet data into dataframe
    # parquetファルのdataframeへのロード
    df = spark.read.option("header",True).parquet(f"s3a://{SOURCE_BUCKET}/nyc-taxi/yellow_tripdata_2022-01.parquet")
    # write the dataframe into an Iceberg table
    # Icebergテーブルへdataframeのデータを書き込み
    df.writeTo(f" {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022").create()
    # describe the table created
    # 作成したテーブルをdescribe
    spark.sql(f'describe table  {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022').show(25)
    # query the table
    # テーブルの照会　
    spark.sql(f'select * from  {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022').show()

def ingest_from_csv_temp_table(spark):
    # load csv data into a dataframe
    # csvファイルのdataframeへのロード
    csvDF = spark.read.option("header",True).csv(f"s3a://{SOURCE_BUCKET}/zipcodes.csv")
    csvDF.createOrReplaceTempView("tempCSVTable")
    # load temporary table into an Iceberg table
    # テンポラリーテーブルからテーブル作成
    spark.sql(f'create or replace table  {CATALOG_NAME}.{SCHEMA_NAME}.zipcodes using iceberg as select * from tempCSVTable')
    # describe the table created
    # 作成したテーブルをdescribe
    spark.sql(f'describe table  {CATALOG_NAME}.{SCHEMA_NAME}.zipcodes').show(25)
    # query the table
    # テーブルの照会　（.count()は.showに）修正
    spark.sql(f'select * from  {CATALOG_NAME}.{SCHEMA_NAME}.zipcodes').show()

def ingest_monthly_data(spark):
    # parquetファルのdataframeへのロード
    df_feb = spark.read.option("header",True).parquet(f"s3a://{SOURCE_BUCKET}//nyc-taxi/yellow_tripdata_2022-02.parquet")
    df_march = spark.read.option("header",True).parquet(f"s3a://{SOURCE_BUCKET}//nyc-taxi/yellow_tripdata_2022-03.parquet")
    df_april = spark.read.option("header",True).parquet(f"s3a://{SOURCE_BUCKET}//nyc-taxi/yellow_tripdata_2022-04.parquet")
    df_may = spark.read.option("header",True).parquet(f"s3a://{SOURCE_BUCKET}//nyc-taxi/yellow_tripdata_2022-05.parquet")
    df_june = spark.read.option("header",True).parquet(f"s3a://{SOURCE_BUCKET}//nyc-taxi/yellow_tripdata_2022-06.parquet")

    # dataframeを1つに統合
    df_q1_q2 = df_feb.union(df_march).union(df_april).union(df_may).union(df_june)
    # Icebergテーブルへ統合したdataframeのデータを書き込み
    df_q1_q2.write.insertInto(f" {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022")

def perform_table_maintenance_operations(spark):
    # Query the metadata files table to list underlying data files
    # メタデータ・ファイル・テーブルを照会し、中のデータ・ファイルを一覧表示する。
    spark.sql(f"SELECT file_path, file_size_in_bytes FROM  {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022.files").show()

    # There are many smaller files compact them into files of 200MB each using the
    # `rewrite_data_files` Iceberg Spark procedure
    # 小さいファイルがたくさんあるので、`rewrite_data_files` Iceberg Spark プロシージャを使って、200MBずつファイルにまとめる
    spark.sql(f"CALL {CATALOG_NAME}.system.rewrite_data_files(table => '{SCHEMA_NAME}.yellow_taxi_2022', options => map('target-file-size-bytes','209715200'))").show()

    # Again, query the metadata files table to list underlying data files; 6 files are compacted
    # to 3 files
    # 再度 メタデータ・ファイル・テーブルを照会し、中のデータ・ファイルを一覧表示する。
    # 6ファイルが3ファイルに圧縮されている
    spark.sql(f"SELECT file_path, file_size_in_bytes FROM  {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022.files").show()

    # List all the snapshots
    # Expire earlier snapshots. Only latest one with comacted data is required
    # Again, List all the snapshots to see only 1 left
    # スナップショットの一覧を表示し
    # 初期のスナップショットは破棄し、圧縮させた最新のデータのみにする
    # 再度 スナップショットの一覧を表示し、1つだけあることを確認
    spark.sql(f"SELECT committed_at, snapshot_id, operation FROM  {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022.snapshots").show()
    # retain only the latest one
    # 最新のものだけ残す
    latest_snapshot_committed_at = spark.sql(f"SELECT committed_at, snapshot_id, operation FROM  {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022.snapshots").tail(1)[0].committed_at
    print (latest_snapshot_committed_at)
    spark.sql(f"CALL {CATALOG_NAME}.system.expire_snapshots(table => '{SCHEMA_NAME}.yellow_taxi_2022',older_than => TIMESTAMP '{latest_snapshot_committed_at}',retain_last => 1)").show()
    spark.sql(f"SELECT committed_at, snapshot_id, operation FROM  {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022.snapshots").show()

    # Removing Orphan data files
    # 廃棄された(Orphan)データファイルを削除
    spark.sql(f"CALL {CATALOG_NAME}.system.remove_orphan_files(table => '{SCHEMA_NAME}.yellow_taxi_2022')").show(truncate=False)

    # Rewriting Manifest Files
    # マニフェストファイルの再書き込み
    spark.sql(f"CALL {CATALOG_NAME}.system.rewrite_manifests('{SCHEMA_NAME}.yellow_taxi_2022')").show()


def evolve_schema(spark):
    # demonstration: Schema evolution
    # Add column fare_per_mile to the table
    # スキーマの変更
    # テーブルにfare_per_mileカラムを追加する
    spark.sql(f'ALTER TABLE  {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022 ADD COLUMN(fare_per_mile double)')
    # describe the table
    # テーブルをdescribe
    spark.sql(f'describe table  {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022').show(25)

def clean_database(spark):
    # clean-up the demo database
    spark.sql(f'drop table if exists {CATALOG_NAME}.{SCHEMA_NAME}.testTable purge')
    spark.sql(f'drop table if exists {CATALOG_NAME}.{SCHEMA_NAME}.zipcodes purge')
    spark.sql(f'drop table if exists {CATALOG_NAME}.{SCHEMA_NAME}.yellow_taxi_2022 purge')
    spark.sql(f'drop database if exists {CATALOG_NAME}.{SCHEMA_NAME} cascade')

def main():
    try:
        spark = init_spark()

        # 以下必要なものだけ実行するとよい
        # 必要ないものはコメントアウトする

        # このプログラムで以前作成したテーブルを最初に削除してから実行したい場合はコメントを外す
        # clean_database(spark)

        # スキーマの作成
        create_database(spark)

        # スキーマの一覧
        list_databases(spark)

        # Icebergテーブル基本操作
        basic_iceberg_table_operations(spark)

        # demonstration: Ingest parquet and csv data into a wastonx.data Iceberg table
        # デモ: parquetデータとcsvデータをwastonx.data Icebergテーブルに取り込む
        create_table_from_parquet_data(spark)
        ingest_from_csv_temp_table(spark)

        # load data for the month of Feburary to June into the table yellow_taxi_2022 created above
        # 上で作成したyellow_taxi_2022テーブルに、2月から6月までのデータをロードする。
        ingest_monthly_data(spark)

        # demonstration: Table maintenance
        # デモ: テーブル保守
        perform_table_maintenance_operations(spark)

        # demonstration: Schema evolution
        # デモ: スキーマの変更
        evolve_schema(spark)

    finally:
        # clean-up the demo database
        # 以下は作成テーブルが削除される。実行結果がわからなくなるので、削除したい場合のみコメントを外す
        # clean_database(spark)
        spark.stop()

if __name__ == '__main__':
  main()