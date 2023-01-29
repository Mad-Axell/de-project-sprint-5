class ConfigConst:
    #PG_WAREHOUSE_CONNECTION = 'PG_WAREHOUSE_CONNECTION'
    #PG_ORIGIN_BONUS_SYSTEM_CONNECTION = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'

    MONGO_DB_CERTIFICATE_PATH = '/opt/airflow/certificates/PracticumSp5MongoDb.crt'
    MONGO_DB_USER = 'student'
    MONGO_DB_PASSWORD = 'student1'
    MONGO_DB_REPLICA_SET = 'rs01'
    MONGO_DB_DATABASE_NAME = 'db-mongo'
    MONGO_DB_HOST = 'rc1a-ba83ae33hvt4pokq.mdb.yandexcloud.net:27018'
    
    PG_ORIGIN_HOST = 'rc1a-1kn18k47wuzaks6h.mdb.yandexcloud.net'
    PG_ORIGIN_DATABASE_NAME = 'de-public'
    PG_ORIGIN_PORT = '6432'
    PG_ORIGIN_USER = 'student'
    PG_ORIGIN_PASSWORD = 'student1'
    PG_ORIGIN_SSLMODE = 'require'
    
    PG_WAREHOUSE_HOST = 'localhost'
    PG_WAREHOUSE_DATABASE_NAME = 'de'
    PG_WAREHOUSE_PORT = '5432'
    PG_WAREHOUSE_USER = 'jovyan'
    PG_WAREHOUSE_PASSWORD = 'jovyan'
    PG_WAREHOUSE_SSLMODE = 'disable'
    
    REST_HEADERS =  {
                        "X-API-KEY":    '25c27781-8fde-4b30-a22e-524044a7580f',
                        "X-Nickname":   'MadAxell',
                        "X-Cohort":     '8'
                    }