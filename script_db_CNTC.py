#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 10 15:20:42 2022

@author: celia
"""


import psycopg2
import yaml
import pandas as pd
import csv
from sqlalchemy import create_engine
import os


# Pour load le yaml fichiers avec paramètres : !!! Attention changer le chemin sur votre machine !!!
with open('/home/celia/Documents/Brazilian_ecom/config.yml','r') as f:
    conf= yaml.safe_load(f)
my = conf['PG']


# Connexion à la base de données postgresql
conn = psycopg2.connect(host=my['host'],
                user=my['user'],
                password =my['password'],
                port=my['port'],
                database=my['database'])

conn.autocommit = True
cursor = conn.cursor()
print("Database opened successfully")



# Vide les tables
cursor.execute(''' DROP TABLE  IF EXISTS olist_sellers_dataset CASCADE''')
cursor.execute(''' DROP TABLE  IF EXISTS olist_geolocation_dataset CASCADE''')
cursor.execute(''' DROP TABLE  IF EXISTS olist_customers_dataset CASCADE''')
cursor.execute(''' DROP TABLE  IF EXISTS olist_orders_dataset CASCADE''')
cursor.execute(''' DROP TABLE  IF EXISTS olist_order_payments_dataset CASCADE''')
cursor.execute(''' DROP TABLE  IF EXISTS olist_order_reviews_dataset CASCADE''')
cursor.execute(''' DROP TABLE  IF EXISTS olist_order_items_dataset CASCADE''')
cursor.execute(''' DROP TABLE  IF EXISTS olist_sellers_dataset CASCADE''')
cursor.execute(''' DROP TABLE  IF EXISTS olist_products_dataset CASCADE''')
cursor.execute(''' DROP TABLE  IF EXISTS product_category_name_translation CASCADE''')


# ETAPE 1

#Creation des tables et ajout des types, clés primaires

cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_geolocation_dataset
(
    geolocation_zip_code_prefix character varying(8) COLLATE pg_catalog."default" NOT NULL,
    geolocation_lat double precision,
    geolocation_lng double precision,
    geolocation_city character varying(38) COLLATE pg_catalog."default",
    geolocation_state character varying(2) COLLATE pg_catalog."default",
    CONSTRAINT geolocation_pkey PRIMARY KEY (geolocation_zip_code_prefix)
);""")

print("Table olist_geolocation_dataset created successfully ")


#-------------------------------

cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_sellers_dataset
(
    seller_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    seller_zip_code_prefix character varying(8) COLLATE pg_catalog."default",
    seller_city character varying(40) COLLATE pg_catalog."default",
    seller_state character varying(2) COLLATE pg_catalog."default",
    CONSTRAINT olist_sellers_dataset_pkey PRIMARY KEY (seller_id)
);""")

print("Table olist_sellers_dataset created successfully ")


#---------------------------------


cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_customers_dataset
(
    customer_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    customer_unique_id character varying(32) COLLATE pg_catalog."default",
    customer_zip_code_prefix character varying(8) COLLATE pg_catalog."default",
    customer_city character varying(32) COLLATE pg_catalog."default",
    customer_state character varying(2) COLLATE pg_catalog."default",
    CONSTRAINT olist_customers_dataset_pkey PRIMARY KEY (customer_id) 
);""")

print("Table olist_customers_dataset created successfully ")


#-------------------------------


cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_orders_dataset
(
    order_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    customer_id character varying(32) COLLATE pg_catalog."default",
    order_status character varying(20) COLLATE pg_catalog."default",
    order_purchase_timestamp timestamp without time zone,
    order_approved_at timestamp without time zone,
    order_delivered_carrier_date timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date timestamp without time zone,
    CONSTRAINT olist_orders_dataset_pkey PRIMARY KEY (order_id)    );""")


print("Table olist_orders_dataset created successfully ")


#--------------------------------------


cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_order_payments_dataset
(
    order_id character varying(32) COLLATE pg_catalog."default",
    payment_sequential integer,
    payment_type character varying(12) COLLATE pg_catalog."default",
    payment_installments integer,
    payment_value double precision,
    CONSTRAINT olist_payment_dataset_pkey PRIMARY KEY (order_id, payment_sequential));""")

print("Table olist_order_payments_dataset created successfully ")

#---------------------------------------


cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_order_reviews_dataset
(
    review_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    order_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    review_score integer,
    review_comment_title character varying(36) COLLATE pg_catalog."default",
    review_comment_message character varying(270) COLLATE pg_catalog."default",
    review_creation_date timestamp without time zone,
    review_answer_timestamp timestamp without time zone,
    CONSTRAINT olist_order_reviews_dataset_pkey PRIMARY KEY (review_id, order_id));""")

print("Table olist_order_reviews_dataset created successfully ")

#--------------------------

cursor.execute(""" CREATE TABLE IF NOT EXISTS public.product_category_name_translation 
               (
               product_category_name character varying(60) COLLATE pg_catalog."default" NOT NULL,
               product_category_name_english character varying(60) COLLATE pg_catalog."default",
               CONSTRAINT translation_pkey PRIMARY KEY (product_category_name)
               );""")

               
print("Table product_category_name_translation created successfully ")


#-------------------------------

cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_products_dataset
(
    product_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    product_category_name character varying(46) COLLATE pg_catalog."default",
    product_name_lenght double precision,
    product_description_lenght double precision,
    product_photos_qty double precision,
    product_weight_g double precision,
    product_length_cm double precision,
    product_height_cm double precision,
    product_width_cm double precision,
    CONSTRAINT olist_products_dataset_pkey PRIMARY KEY (product_id)
);""")

print("Table olist_products_dataset created successfully ")

#-------------------------------


cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_order_items_dataset
(
    order_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    order_item_id integer NOT NULL,
    product_id character varying(32) COLLATE pg_catalog."default",
    seller_id character varying(32) COLLATE pg_catalog."default",
    shipping_limit_date timestamp without time zone,
    price double precision,
    freight_value double precision,
    CONSTRAINT olist_items_dataset_pkey PRIMARY KEY (order_id, order_item_id)

);""")

print("Table olist_order_items_dataset created successfully ")



# ETAPE 2

#Import des données csv dans les tables  

#!!!!! Attention les données csv doivent être stockées sur le serveur  "/raid/home/youruser/olist....csv"
# Faire une copie du csv depuis votre machine jusqu'au serveur: 
# commande à réaliser sur le terminal : scp /home/youruser/Documents/Brazilian_ecom/olist_.......csv user@datalab-mame.myconnectech.fr:/raid/home/youruser

cursor.execute('''COPY olist_orders_dataset 
FROM '/raid/home/mato/olist_orders_dataset.csv'
DELIMITER ','
CSV HEADER;''')

print(" olist_orders_dataset csv successfully imported")


cursor.execute('''COPY olist_geolocation_dataset
FROM '/raid/home/mato/olist_geolocation_dataset.csv'
DELIMITER ','
CSV HEADER;''')

print(" olist_geolocation_dataset csv successfully imported")



cursor.execute('''COPY olist_order_items_dataset 
FROM '/raid/home/mato/olist_order_items_dataset.csv'
DELIMITER ','
CSV HEADER;''')

print(" olist_order_items_dataset csv successfully imported")


cursor.execute('''COPY olist_products_dataset 
FROM '/raid/home/mato/olist_products_dataset.csv'
DELIMITER ','
CSV HEADER;''')

print(" olist_products_dataset csv successfully imported")

cursor.execute('''COPY olist_order_payments_dataset 
FROM '/raid/home/mato/olist_order_payments_dataset.csv'
DELIMITER ','
CSV HEADER;''')

print(" olist_order_payments_dataset csv successfully imported")



cursor.execute('''COPY olist_order_reviews_dataset 
FROM '/raid/home/mato/olist_order_reviews_dataset.csv'
DELIMITER ','
CSV HEADER;''')

print(" olist_order_reviews_dataset csv successfully imported")


cursor.execute('''COPY product_category_name_translation 
FROM '/raid/home/mato/product_category_name_translation.csv'
DELIMITER ','
CSV HEADER;''')

print(" product_category_name_translation csv successfully imported")


cursor.execute('''COPY olist_sellers_dataset 
FROM '/raid/home/mato/olist_sellers_dataset.csv'
DELIMITER ','
CSV HEADER;''')

print(" olist_sellers_dataset csv successfully imported")


cursor.execute('''COPY olist_customers_dataset 
FROM '/raid/home/mato/olist_customers_dataset.csv'
DELIMITER ','
CSV HEADER;''')

print(" olist_customers_dataset csv successfully imported")




# ETAPE 3

#Import des clés étrangères 

cursor.execute('''
ALTER TABLE IF EXISTS public.olist_customers_dataset
    ADD CONSTRAINT customers_geolocalization FOREIGN KEY (customer_zip_code_prefix)
    REFERENCES public.olist_geolocation_dataset (geolocation_zip_code_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
''')

cursor.execute('''
ALTER TABLE IF EXISTS public.olist_order_items_dataset
    ADD CONSTRAINT order_item_products FOREIGN KEY (product_id)
    REFERENCES public.olist_products_dataset (product_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
''')

cursor.execute('''
ALTER TABLE IF EXISTS public.olist_order_items_dataset
    ADD CONSTRAINT order_item_sellers FOREIGN KEY (seller_id)
    REFERENCES public.olist_sellers_dataset (seller_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
''')


cursor.execute(''' ALTER TABLE IF EXISTS public.olist_order_items_dataset
    ADD CONSTRAINT order_items_orders FOREIGN KEY (order_id)
    REFERENCES public.olist_orders_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
 ''')


cursor.execute(''' ALTER TABLE IF EXISTS public.olist_order_payments_dataset
    ADD CONSTRAINT paiments_order FOREIGN KEY (order_id)
    REFERENCES public.olist_orders_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
 ''')
 
cursor.execute(''' ALTER TABLE IF EXISTS public.olist_order_reviews_dataset
    ADD CONSTRAINT review_order FOREIGN KEY (order_id)
    REFERENCES public.olist_orders_dataset (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
  ''')
 
    
cursor.execute(''' 
ALTER TABLE IF EXISTS public.olist_orders_dataset
    ADD CONSTRAINT order_customer FOREIGN KEY (customer_id)
    REFERENCES public.olist_customers_dataset (customer_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
  ''')
  
  
cursor.execute(''' 
ALTER TABLE IF EXISTS public.olist_products_dataset
    ADD CONSTRAINT traslations_products FOREIGN KEY (product_category_name)
    REFERENCES public.product_category_name_translation (product_category_name) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

  ''')
  
cursor.execute(''' 
ALTER TABLE IF EXISTS public.olist_sellers_dataset
    ADD CONSTRAINT sellers_geolocalization FOREIGN KEY (seller_zip_code_prefix)
    REFERENCES public.olist_geolocation_dataset (geolocation_zip_code_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

  ''')


cursor.close()
conn.commit()

conn.close()
