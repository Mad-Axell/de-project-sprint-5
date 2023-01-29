import logging
import time

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib import ConnectionBuilder, MongoConnect, ApiConnect
from utility.config_const import ConfigConst
from utility.dwh_schema_ddl import SchemaDdl

from stg.models.pg_ranks_loader import PG_RankLoader
from stg.models.pg_users_loader import PG_UserLoader
from stg.models.pg_events_loader import PG_EventLoader
from stg.models.mg_restaurants_loader import MG_RestaurantLoader, RestaurantOriginRepository, RestaurantDestRepository
from stg.models.mg_users_loader import MG_UserLoader, UserOriginRepository, UserDestRepository
from stg.models.mg_orders_loader import MG_OrderLoader, OrderOriginRepository, OrderDestRepository

from stg.models.rest_couriers_loader import REST_CourierLoader, CourierDestRepository, CourierOriginRepository
from stg.models.rest_deliveries_loader import REST_DeliveryLoader, DeliveryDestRepository, DeliveryOriginRepository


from dds.models.loaders.user_loader import UserLoader
from dds.models.loaders.restaurant_loader import RestaurantLoader
from dds.models.loaders.timestamp_loader import TimestampLoader
from dds.models.loaders.product_loader import ProductLoader
from dds.models.loaders.order_loader import OrderLoader

from dds.models.loaders.courier_loader_simple import CouriersLoader
from dds.models.loaders.fct_deliveries_simple import FctDeliveriesLoad
#from dds.models.loaders.courier_loader_simple import CouriersLoader


from dds.models.loaders.fct_products_loader import FctProductsLoader
from dds.dds_settings_repository import DdsEtlSettingsRepository

from cdm.models.loaders.restaurant_settlement_report_loader import RestaurantSettlementReportLoader
from cdm.models.loaders.courier_settlement_report_loader_simple import CourierSettlementReportLoaderSimple

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@dag(
    dag_id='PipeLine_Dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['All', 'PipeLine'],
    is_paused_upon_creation=False
)
def pipeline_dag():
    
    
    dwh_pg_connect = ConnectionBuilder.pg_conn( ConfigConst.PG_WAREHOUSE_HOST, 
                                                ConfigConst.PG_WAREHOUSE_PORT,
                                                ConfigConst.PG_WAREHOUSE_DATABASE_NAME,
                                                ConfigConst.PG_WAREHOUSE_USER,
                                                ConfigConst.PG_WAREHOUSE_PASSWORD,
                                                ConfigConst.PG_WAREHOUSE_SSLMODE)
                                                
    origin_pg_connect = ConnectionBuilder.pg_conn(  ConfigConst.PG_ORIGIN_HOST, 
                                                    ConfigConst.PG_ORIGIN_PORT,
                                                    ConfigConst.PG_ORIGIN_DATABASE_NAME,
                                                    ConfigConst.PG_ORIGIN_USER,
                                                    ConfigConst.PG_ORIGIN_PASSWORD,
                                                    ConfigConst.PG_ORIGIN_SSLMODE)
    
    origin_mongo_connect = MongoConnect(ConfigConst.MONGO_DB_CERTIFICATE_PATH, 
                                        ConfigConst.MONGO_DB_USER, 
                                        ConfigConst.MONGO_DB_PASSWORD, 
                                        ConfigConst.MONGO_DB_HOST, 
                                        ConfigConst.MONGO_DB_REPLICA_SET, 
                                        ConfigConst.MONGO_DB_DATABASE_NAME, 
                                        ConfigConst.MONGO_DB_DATABASE_NAME)
    
    
    
    settings_repository = DdsEtlSettingsRepository()
    
    @task(task_id="Connect")
    def Connect():
        time.sleep(0)
        
    @task(task_id="schema_init")
    def schema_init(ds=None, **kwargs):
        schema_loader = SchemaDdl(dwh_pg_connect)
        schema_loader.init_schema()
    
    @task(task_id="pg_ranks_load")
    def pg_ranks_load():
        rank_loader = PG_RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rank_loader.load_ranks()
    
    @task(task_id="pg_users_load")
    def pg_users_load():
        user_loader = PG_UserLoader(origin_pg_connect, dwh_pg_connect, log)
        user_loader.load_users()
    
    @task(task_id="pg_events_load")
    def pg_events_load():
        event_loader = PG_EventLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_events()
    
    @task(task_id="mg_restaurants_load")
    def mg_restaurants_load():
        restaurants_pg_saver = RestaurantDestRepository()
        restaurants_collection_reader = RestaurantOriginRepository(origin_mongo_connect)
        restaurants_loader = MG_RestaurantLoader(restaurants_collection_reader, dwh_pg_connect, restaurants_pg_saver, log)
        restaurants_loader.load_restaurant()
    
    @task(task_id="mg_users_load")
    def mg_users_load():
        users_pg_saver = UserDestRepository()
        users_collection_reader = UserOriginRepository(origin_mongo_connect)
        users_loader = MG_UserLoader(users_collection_reader, dwh_pg_connect, users_pg_saver, log)
        users_loader.load_user()
        
    @task(task_id="mg_orders_load")
    def mg_orders_load():
        orders_pg_saver = OrderDestRepository()
        orders_collection_reader = OrderOriginRepository(origin_mongo_connect)
        orders_loader = MG_OrderLoader(orders_collection_reader, dwh_pg_connect, orders_pg_saver, log)
        orders_loader.load_order()
        
    #----------------------------------------------------------------------------------------------------------
    #----------------------------------------------------------------------------------------------------------
    #----------------------------------------------------------------------------------------------------------    

    @task(task_id="rest_couriers_load")
    def rest_couriers_load():
        couriers_pg_saver = CourierDestRepository()
        couriers_collection_reader = CourierOriginRepository()
        couriers_loader = REST_CourierLoader(couriers_collection_reader, dwh_pg_connect, couriers_pg_saver, log)
        couriers_loader.load_courier()
    
    @task(task_id="rest_deliveries_load")
    def rest_deliveries_load():
        deliveries_pg_saver = DeliveryDestRepository()
        deliveries_collection_reader = DeliveryOriginRepository()
        deliveries_loader = REST_DeliveryLoader(deliveries_collection_reader, dwh_pg_connect, deliveries_pg_saver, log)
        deliveries_loader.load_delivery()
    
    @task(task_id="dm_couriers_load")
    def dm_couriers_load(ds=None, **kwargs):
        courier_loader = CourierLoader(dwh_pg_connect, settings_repository)
        courier_loader.load_couriers()
    
    
    #----------------------------------------------------------------------------------------------------------
    #----------------------------------------------------------------------------------------------------------
    #----------------------------------------------------------------------------------------------------------         
    
    @task(task_id="dm_users_load")
    def dm_users_load(ds=None, **kwargs):
        user_loader = UserLoader(dwh_pg_connect, settings_repository)
        user_loader.load_users()

    @task(task_id="dm_restaurants_load")
    def dm_restaurants_load(ds=None, **kwargs):
        restaurant_loader = RestaurantLoader(dwh_pg_connect, settings_repository)
        restaurant_loader.load_restaurants()
    
    @task(task_id="dm_timestamps_load")
    def dm_timestamps_load(ds=None, **kwargs):
        ts_loader = TimestampLoader(dwh_pg_connect, settings_repository)
        ts_loader.load_timestamps()
    
    @task(task_id="dm_products_load")
    def dm_products_load(ds=None, **kwargs):
        prod_loader = ProductLoader(dwh_pg_connect, settings_repository)
        prod_loader.load_products()
    
    @task(task_id="dm_orders_load")
    def dm_orders_load(ds=None, **kwargs):
        order_loader = OrderLoader(dwh_pg_connect, settings_repository)
        order_loader.load_orders()
    
    @task(task_id="fct_order_products_load")
    def fct_order_products_load(ds=None, **kwargs):
        fct_loader = FctProductsLoader(dwh_pg_connect, settings_repository)
        fct_loader.load_product_facts()
        
    @task(task_id="restaurant_settlement_report")
    def restaurant_settlement_report():
        restaurant_settlement_report_load = RestaurantSettlementReportLoader(dwh_pg_connect)
        restaurant_settlement_report_load.load_report_by_days()  
#-----------------------------------------------------------------------------------------------------------------------
#-------БУДЕМ-ГОВНОКОДИТЬ-----------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
    @task(task_id="dm_couriers_load")
    def dm_couriers_load():
        couriers_loader = CouriersLoader(dwh_pg_connect)
        couriers_loader.load_couriers()

    @task(task_id="fct_deliveries_load")
    def fct_deliveries_load():
        fct_deliveries_loader = FctDeliveriesLoad(dwh_pg_connect)
        fct_deliveries_loader.load_fct_deliveries()
    
    @task(task_id="courier_settlement_report")
    def courier_settlement_report():
        courier_settlement_report_load = CourierSettlementReportLoaderSimple(dwh_pg_connect)
        courier_settlement_report_load.load_report()
    

#-----------------------------------------------------------------------------------------------------------------------
#-------НЕ-БУДЕМ-ГОВНОКОДИТЬ---------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------        
    
    # Инициализируем объявленные таски.
    schema_reload = schema_init()
    #schema_reload # type: ignore
    
    task_connector_1 = Connect()
    #task_connector_1 # type: ignore
    
    task_connector_2 = Connect()
    task_connector_3 = Connect()
    #task_connector_2 # type: ignore

    pg_ranks = pg_ranks_load()
    #pg_ranks  # type: ignore
    
    pg_users = pg_users_load()
    #pg_users  # type: ignore
    
    pg_events = pg_events_load()
    #pg_events  # type: ignore
    
    mg_restaurants = mg_restaurants_load()
    #mg_restaurants # type: ignore
    
    mg_users = mg_users_load()
    #mg_users # type: ignore
    
    mg_orders = mg_orders_load()
    #mg_orders # type: ignore
    
    rest_couriers = rest_couriers_load()
    rest_deliveries = rest_deliveries_load()
    
    dm_couriers = dm_couriers_load()
    fct_deliveries = fct_deliveries_load()
    
    dm_users = dm_users_load()
    #dm_users # type: ignore
    
    dm_restaurants = dm_restaurants_load()
    #dm_restaurants # type: ignore
    
    dm_timestamps = dm_timestamps_load()
    #dm_timestamps # type: ignore
    
    dm_products = dm_products_load()
    #dm_products # type: ignore
    
    dm_orders = dm_orders_load()
    #dm_orders # type: ignore
    
    fct_order_products = fct_order_products_load()
    #fct_order_products # type: ignore
    
    restaurant_settlement_load = restaurant_settlement_report()
    #restaurant_settlement_load # type: ignore
    
    courier_settlement_load = courier_settlement_report()
    
    (
    schema_reload 
    >> [pg_ranks, pg_users, pg_events, mg_restaurants, mg_users, mg_orders, rest_couriers, rest_deliveries] 
    >> task_connector_1 
    >> [dm_users, dm_restaurants, dm_timestamps, dm_couriers] 
    >> task_connector_2 
    >> [dm_products, dm_orders] 
    >> task_connector_3
    >> [fct_order_products, fct_deliveries]
    >> restaurant_settlement_load
    >> courier_settlement_load
    )
pipeline_dag = pipeline_dag()