from pyspark.sql import SparkSession

path = '/opt/airflow/dags/'
st_path = '/opt/airflow/data_staging'


def extract(path):
    '''
    Fungsi ini ditujukan untuk mengambil data dari local file untuk selanjutnya dilakukan transform (data cleaning)
    parameter:
        path : lokasi file yang akan diekstrak
    return:
        data : dataframe spark
    '''

    # Membuat spark engine
    spark = SparkSession.builder.getOrCreate()

    # Membuat datafram menggunakan spark engine pada path yang telah ditentukan
    data = spark.read.csv(f'{path}/merch_sales.csv', header=True, inferSchema=True)

    # Mengubah nama file dataframe dengan path spesifik
    data.toPandas().to_csv(f'{st_path}/data_raw.csv', index=False)
    
    return data

if __name__ == '__main__':
    extract(path)