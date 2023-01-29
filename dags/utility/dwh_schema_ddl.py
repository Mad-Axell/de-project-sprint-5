from lib import PgConnect


class SchemaDdl:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_schema(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
-- -------------------------------------------------------------------------------------------------------------
-- Create CDM layer
-- -------------------------------------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS cdm CASCADE;
CREATE SCHEMA IF NOT EXISTS cdm AUTHORIZATION jovyan;

drop table if exists de.cdm.dm_restaurant_settlement_report CASCADE;
create table if not exists de.cdm.dm_restaurant_settlement_report
(
	id serial not null,
	restaurant_id varchar not null,
	restaurant_name varchar not null,
	settlement_date date not null,
	orders_count integer not null DEFAULT(0),
	orders_total_sum numeric(14, 2)not null DEFAULT(0),
	orders_bonus_payment_sum numeric(14, 2 )not null DEFAULT(0),
	orders_bonus_granted_sum numeric(14, 2) not null DEFAULT(0),
	order_processing_fee numeric(14, 2) not null DEFAULT(0),
	restaurant_reward_sum numeric(14, 2) not null DEFAULT(0),
	CONSTRAINT dm_restaurant_settlement_report_pk primary key (id),
	CONSTRAINT dm_restaurant_settlement_report_orders_count_check check (orders_count >= 0),
	CONSTRAINT dm_restaurant_settlement_report_orders_total_sum_check check (orders_total_sum >= 0),
	CONSTRAINT dm_restaurant_settlement_report_orders_bonus_payment_sum_check check (orders_bonus_payment_sum >= 0),
	CONSTRAINT dm_restaurant_settlement_report_orders_bonus_granted_sum_check check (orders_bonus_granted_sum >= 0),
	CONSTRAINT dm_restaurant_settlement_report_order_processing_fee_check check (order_processing_fee >= 0),
	CONSTRAINT dm_restaurant_settlement_report_restaurant_reward_sum_check check (restaurant_reward_sum >= 0),
	CONSTRAINT dm_restaurant_settlement_report_restaurant_unique UNIQUE(restaurant_id, settlement_date)
);

drop table if exists de.cdm.dm_courier_settlement_report CASCADE;
CREATE TABLE IF NOT EXISTS de.cdm.dm_courier_settlement_report
(
	id	serial,
	courier_id	integer,
	courier_name	varchar,
	settlement_year integer not null,
    settlement_month integer not null,
	orders_count	integer,
	orders_total_sum	numeric(14, 2),
	rate_avg	NUMERIC,
	order_processing_fee	NUMERIC,
	courier_order_sum	numeric(14, 2),
	courier_tips_sum	numeric(14, 2),
	courier_reward_sum	numeric(14, 2),
    CONSTRAINT dm_courier_settlement_report_pk primary key (id), 
	CONSTRAINT dm_courier_settlement_report_courier_unique UNIQUE (courier_id, settlement_year, settlement_month), 
	CONSTRAINT dm_courier_settlement_report_orders_count_check check (orders_count >= 0), 
	CONSTRAINT dm_courier_settlement_report_orders_total_sum_check check (orders_total_sum >= 0), 
	CONSTRAINT dm_courier_settlement_report_rate_avg_check check (rate_avg >= 0), 
	CONSTRAINT dm_courier_settlement_report_order_processing_fee_check check (order_processing_fee >= 0), 
	CONSTRAINT dm_courier_settlement_report_courier_order_sum_check check (courier_order_sum >= 0), 
	CONSTRAINT dm_courier_settlement_report_courier_tips_sum_check check (courier_tips_sum >= 0), 
	CONSTRAINT dm_courier_settlement_report_courier_reward_sum_check check (courier_reward_sum >= 0)
);


-- -------------------------------------------------------------------------------------------------------------
-- Create DDS layer
-- -------------------------------------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS dds CASCADE;
CREATE SCHEMA IF NOT EXISTS dds AUTHORIZATION jovyan;

DROP TABLE IF EXISTS de.dds.dm_users CASCADE;
CREATE TABLE IF NOT EXISTS de.dds.dm_users
(
	id serial CONSTRAINT dm_users_pk PRIMARY KEY,
	user_id VARCHAR NOT NULL,
	user_name VARCHAR NOT NULL,
	user_login VARCHAR NOT NULL
);

DROP TABLE IF EXISTS de.dds.dm_restaurants CASCADE;
CREATE TABLE IF NOT EXISTS de.dds.dm_restaurants
(
	id serial CONSTRAINT dm_restaurants_pk PRIMARY KEY,
	restaurant_id VARCHAR NOT NULL,
	restaurant_name VARCHAR NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL
);


DROP TABLE IF EXISTS de.dds.dm_timestamps CASCADE;
CREATE TABLE IF NOT EXISTS de.dds.dm_timestamps
(
	id serial CONSTRAINT dm_timestamps_pk PRIMARY KEY,
	ts timestamp NOT NULL,
	year smallint NOT NULL CONSTRAINT dm_timestamps_year_check CHECK (year >= 2022 AND year < 2500),
	month smallint  NOT NULL CONSTRAINT dm_timestamps_month_check CHECK (month >= 1 AND month <= 12),
	day smallint NOT NULL CONSTRAINT dm_timestamps_day_check CHECK (day >= 1 AND day <= 31),
	time time NOT NULL,
	date date NOT null,
	CONSTRAINT dm_timestamps_unque UNIQUE (ts)
);

DROP TABLE IF EXISTS de.dds.dm_products CASCADE;
CREATE TABLE IF NOT EXISTS de.dds.dm_products
(
	id serial CONSTRAINT dm_products_pk PRIMARY KEY,
	restaurant_id INTEGER NOT NULL,
	product_id VARCHAR NOT NULL,
	product_name VARCHAR NOT NULL,
	product_price NUMERIC(14,2) NOT NULL DEFAULT 0,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_product_price_check_1 CHECK (product_price >= 0),
	CONSTRAINT dm_products_product_price_check_2 CHECK (product_price < 1000000000000),
	CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES de.dds.dm_restaurants (id)
);



DROP TABLE IF EXISTS de.dds.dm_orders CASCADE;
CREATE TABLE IF NOT EXISTS de.dds.dm_orders
(
	id serial CONSTRAINT dm_orders_pk PRIMARY KEY,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	user_id INTEGER NOT NULL,
	restaurant_id INTEGER NOT NULL,
	timestamp_id INTEGER NOT NULL,
	CONSTRAINT dm_orders_user_id_fk FOREIGN KEY (user_id) REFERENCES de.dds.dm_users (id),
	CONSTRAINT dm_orders_restaurant_id_fk FOREIGN KEY (restaurant_id) REFERENCES de.dds.dm_restaurants (id),
	CONSTRAINT dm_orders_timestamp_id_fk FOREIGN KEY (timestamp_id) REFERENCES de.dds.dm_timestamps (id),
	CONSTRAINT dm_orders_order_key_unque UNIQUE (order_key)
);

DROP TABLE IF EXISTS de.dds.fct_product_sales CASCADE;
CREATE TABLE IF NOT EXISTS de.dds.fct_product_sales
(
	id serial CONSTRAINT fct_product_sales_pk PRIMARY KEY,
	product_id INTEGER NOT NULL,
	order_id INTEGER NOT NULL,
	count INTEGER NOT NULL DEFAULT 0,
	price numeric(14, 2) NOT NULL DEFAULT 0,
	total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_payment numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_grant numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_product_sales_count_check CHECK (count >= 0),
	CONSTRAINT fct_product_sales_price_check CHECK (price >= 0),
	CONSTRAINT fct_product_sales_total_sum_check CHECK (total_sum >= 0),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK (bonus_payment >= 0),
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK (bonus_grant >= 0),
	CONSTRAINT fct_product_sales_product_id_fk FOREIGN KEY (product_id) REFERENCES de.dds.dm_products (id),
	CONSTRAINT fct_product_sales_order_id_fk FOREIGN KEY (order_id) REFERENCES de.dds.dm_orders (id)
);
CREATE INDEX IF NOT EXISTS IDX_fct_product_sales__product_id ON dds.fct_product_sales (product_id);
CREATE UNIQUE INDEX IF NOT EXISTS IDX_fct_product_sales__order_id_product_id ON dds.fct_product_sales (order_id, product_id);

DROP TABLE IF EXISTS de.dds.srv_wf_settings CASCADE;
CREATE TABLE IF NOT EXISTS de.dds.srv_wf_settings 
(
    id serial NOT NULL,
    workflow_key varchar NOT NULL,
    workflow_settings JSON NOT null,
    CONSTRAINT dds_srv_wf_settings_pk PRIMARY KEY (id),
    CONSTRAINT dds_srv_wf_workflow_settings_unique UNIQUE (workflow_key)
);

DROP TABLE IF EXISTS de.dds.dm_couriers CASCADE;
CREATE TABLE IF NOT EXISTS de.dds.dm_couriers(
	id serial,
	courier_id varchar,
	name varchar,
    CONSTRAINT dds_dm_couriers_id_pk PRIMARY KEY (id),
    CONSTRAINT dds_dm_couriers_courier_id_unique UNIQUE (courier_id)
);
/*
DROP TABLE IF EXISTS de.dds.dm_deliveries CASCADE;
CREATE TABLE IF NOT EXISTS de.dds.dm_deliveries(
	id serial,
	delivery_id varchar,
    CONSTRAINT dds_dm_deliveries_id_pk PRIMARY KEY (id),
    CONSTRAINT dds_dm_deliveries_delivery_id_unique UNIQUE (delivery_id)
);
*/
DROP TABLE IF EXISTS de.dds.fct_deliveries CASCADE;
CREATE TABLE IF NOT EXISTS de.dds.fct_deliveries(
    id serial,
	delivery_id	varchar NOT NULL,
	courier_id	integer NOT NULL,
	order_id integer NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_ts timestamp NOT NULL,
	address varchar NOT NULL,
	rate  integer NOT NULL,
	tip_sum numeric (14, 2) NOT NULL,
	total_sum numeric (14, 2) NOT NULL,
    CONSTRAINT dds_fct_deliveries_id_pk PRIMARY KEY (id),
    CONSTRAINT dds_fct_deliveries_order_id_dwh_fk FOREIGN KEY (order_id) REFERENCES de.dds.dm_orders(id),
    CONSTRAINT dds_fct_deliveries_courier_id_dwh_fkey FOREIGN KEY (courier_id) REFERENCES de.dds.dm_couriers(id)
);

-- -------------------------------------------------------------------------------------------------------------
-- Create STG layer
-- -------------------------------------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS stg CASCADE;
CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION jovyan;

DROP TABLE if exists de.stg.bonussystem_users;
CREATE TABLE if not exists de.stg.bonussystem_users (
	id int NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT stg_bonussystem_users_pk PRIMARY KEY (id)
);

DROP TABLE if exists de.stg.bonussystem_ranks CASCADE;
CREATE TABLE if not exists de.stg.bonussystem_ranks (
	id int NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) NOT NULL DEFAULT 0,
	min_payment_threshold numeric(19, 5) NOT NULL DEFAULT 0,
	CONSTRAINT stg_bonussystem_ranks_pk PRIMARY KEY (id)
);

DROP TABLE if exists de.stg.bonussystem_events CASCADE;
CREATE TABLE if not exists de.stg.bonussystem_events (
	id int NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value varchar NOT NULL,
	CONSTRAINT stg_bonussystem_events_pk PRIMARY KEY (id)
);
CREATE INDEX idx_bonussystem_events__event_ts ON de.stg.bonussystem_events USING btree (event_ts);


DROP TABLE if exists de.stg.ordersystem_users CASCADE;
CREATE TABLE if not exists de.stg.ordersystem_users (
	id serial NOT NULL,
	object_id varchar NOT NULL,
	object_value varchar NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT stg_ordersystem_users_pk PRIMARY KEY (id),
	CONSTRAINT stg_ordersystem_users_object_id_uindex UNIQUE (object_id)
);

DROP TABLE if exists de.stg.ordersystem_restaurants CASCADE;
CREATE TABLE if not exists de.stg.ordersystem_restaurants (
	id serial NOT NULL,
	object_id varchar NOT NULL,
	object_value varchar NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT stg_ordersystem_restaurants_pk PRIMARY KEY (id),
	CONSTRAINT stg_ordersystem_restaurants_object_id_uindex UNIQUE (object_id)
);

DROP TABLE if exists de.stg.ordersystem_orders CASCADE;
CREATE TABLE if not exists de.stg.ordersystem_orders (
	id serial NOT NULL,
	object_id varchar NOT NULL,
	object_value varchar NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT stg_ordersystem_orders_pk PRIMARY KEY (id),
	CONSTRAINT stg_ordersystem_orders_object_id_uindex UNIQUE (object_id)
);



DROP TABLE if exists de.stg.deliverysystem_couriers CASCADE;
CREATE TABLE IF NOT EXISTS de.stg.deliverysystem_couriers
(
    id serial NOT NULL,
    object_id varchar NOT NULL,
    update_ts timestamp default now() NOT NULL,
    object_value varchar NOT NULL,
    CONSTRAINT stg_deliverysystem_couriers_pk PRIMARY KEY (id),
    CONSTRAINT stg_deliverysystem_couriers_object_id_uindex UNIQUE (object_id)
);


DROP TABLE if exists de.stg.deliverysystem_deliveries CASCADE;
CREATE TABLE IF NOT EXISTS de.stg.deliverysystem_deliveries
(
    id serial NOT NULL,
    object_id varchar NOT NULL,
    update_ts timestamp default now() NOT NULL,
    object_value varchar NOT NULL,
    CONSTRAINT stg_deliverysystem_deliveries_pk PRIMARY KEY (id),
    CONSTRAINT stg_deliverysystem_deliveries_object_id_uindex UNIQUE (object_id)
);

DROP TABLE if exists de.stg.srv_wf_settings CASCADE;
CREATE TABLE IF NOT EXISTS de.stg.srv_wf_settings
(
    id serial NOT NULL,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings varchar NOT null,
    CONSTRAINT stg_srv_wf_settings_pk PRIMARY KEY (id)
);
"""
                )
