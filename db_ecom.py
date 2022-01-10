#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  4 11:32:26 2022

@author: celia
"""


import psycopg2
import yaml
import pandas as pd
import csv
from sqlalchemy import create_engine



# Pour load le yaml fichiers avec paramètres
with open('/home/celia/Bureau/config.yml','r') as f:
    conf= yaml.safe_load(f)
my = conf['PG']


# Connexion à la base de données postgresql
conn = psycopg2.connect(host=my['host'],
                user=my['user'],
                password =my['password'],
                port=my['port'])

conn.autocommit = True
cursor = conn.cursor()
print("Database opened successfully")


#Creation des tables et ajout des types, clés primaires et secondaires

cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_geolocation_dataset
(
    geolocation_zip_code_prefix integer NOT NULL,
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
    seller_zip_code_prefix integer,
    seller_city character varying(40) COLLATE pg_catalog."default",
    seller_state character varying(2) COLLATE pg_catalog."default",
    CONSTRAINT olist_sellers_dataset_pkey PRIMARY KEY (seller_id),
    CONSTRAINT seller_zip_code_fk FOREIGN KEY (seller_zip_code_prefix) REFERENCES public.olist_geolocation_dataset (geolocation_zip_code_prefix)

);""")

print("Table olist_sellers_dataset created successfully ")


#---------------------------------


cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_customers_dataset
(
    customer_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    customer_unique_id character varying(32) COLLATE pg_catalog."default",
    customer_zip_code_prefix integer,
    customer_city character varying(32) COLLATE pg_catalog."default",
    customer_state character varying(2) COLLATE pg_catalog."default",
    CONSTRAINT olist_customers_dataset_pkey PRIMARY KEY (customer_id),
    CONSTRAINT customer_zip_code_fk FOREIGN KEY (customer_zip_code_prefix) REFERENCES public.olist_geolocation_dataset (geolocation_zip_code_prefix)
 
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
    CONSTRAINT olist_orders_dataset_pkey PRIMARY KEY (order_id),
    CONSTRAINT customer_id_order_fk FOREIGN KEY (customer_id) REFERENCES public.olist_customers_dataset (customer_id)
    );""")


print("Table olist_orders_dataset created successfully ")


#--------------------------------------

cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_order_payments_dataset
(
    order_id character varying(32) COLLATE pg_catalog."default",
    payment_sequential integer,
    payment_type character varying(12) COLLATE pg_catalog."default",
    payment_installments integer,
    payment_value double precision,
    CONSTRAINT order_id_payment_fk FOREIGN KEY (order_id) REFERENCES public.olist_orders_dataset (order_id)
);""")

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
    CONSTRAINT olist_order_reviews_dataset_pkey PRIMARY KEY (review_id),
    CONSTRAINT order_id_reviews_fk FOREIGN KEY (order_id) REFERENCES public.olist_orders_dataset (order_id)
);""")

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
    CONSTRAINT olist_products_dataset_pkey PRIMARY KEY (product_id),
    CONSTRAINT translation_fk FOREIGN KEY (product_category_name) REFERENCES public.product_category_name_translation (product_category_name)

);""")

print("Table olist_products_dataset created successfully ")

#-------------------------------


cursor.execute("""CREATE TABLE IF NOT EXISTS public.olist_order_items_dataset
(
    order_id character varying(32) COLLATE pg_catalog."default",
    order_item_id integer,
    product_id character varying(32) COLLATE pg_catalog."default",
    seller_id character varying(32) COLLATE pg_catalog."default",
    shipping_limit_date timestamp without time zone,
    price double precision,
    freight_value double precision,
    CONSTRAINT order_id_item_fk FOREIGN KEY (order_id) REFERENCES public.olist_orders_dataset (order_id),
    CONSTRAINT product_id_item_fk FOREIGN KEY (product_id) REFERENCES public.olist_products_dataset (product_id),
    CONSTRAINT seller_id_item_fk FOREIGN KEY (seller_id) REFERENCES public.olist_sellers_dataset (seller_id)

);""")

print("Table olist_order_items_dataset created successfully ")


#-------------------------------

#Import des données csv dans les tables  
#!!!!! Attention les données csv doivent être stockées sur le serveur  "/raid/home/youruser/olist....csv"
# Faire une copie du csv depuis votre machine jusqu'au serveur: 
# commande a faire sur le terminal : scp /home/youruser/Documents/Brazilian_ecom/olist_.......csv user@datalab-mame.myconnectech.fr:/raid/home/youruser

cursor.execute('''COPY olist_geolocation_dataset 
FROM '/raid/home/mato/olist_geolocation_dataset.csv'
DELIMITER ','
CSV HEADER;''')

print(" olist_geolocation_dataset csv successfully imported")





cursor.close()
conn.commit()
conn.close()
