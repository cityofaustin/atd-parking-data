--
-- PostgreSQL database dump
--

-- Dumped from database version 12.5
-- Dumped by pg_dump version 13.1 (Debian 13.1-1.pgdg100+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


-- custom role declarations no included in pg_dump
create role authenticator noinherit;
create role my_api_user nologin;
grant my_api_user to authenticator;

create role web_anon nologin;
grant web_anon to authenticator;

--
-- Name: api; Type: SCHEMA; Schema: -; Owner: postgres
--
grant usage on schema public to public;
grant create on schema public to public;


CREATE SCHEMA api;


ALTER SCHEMA api OWNER TO postgres;



--
-- Name: trigger_set_updated_at(); Type: FUNCTION; Schema: public; Owner: postgres
--
CREATE TABLE api.transactions (
  "id" int PRIMARY KEY,
  "transaction_id" int,
  "timestamp" timestamp,
  "duration_min" int,
  "start_time" timestamp,
  "end_time" timestamp,
  "amount" real,
  "meter_id" int,
  "payment_method" text
);

CREATE TABLE api.flowbird_transactions_raw (
  "id" bigint PRIMARY KEY,
  "invoice_id" bigint,
  "payment_method" text,
  "meter_id" int,
  "transaction_type" text,
  "timestamp" timestamp,
  "duration_min" real,
  "start_time" timestamp,
  "end_time" timestamp,
  "amount" real,
  "validated" bool
);

CREATE TABLE api.flowbird_payments_raw (
  "invoice_id" bigint PRIMARY KEY,
  "card_type" text,
  "meter_id" int,
  "transaction_type" text,
  "transaction_date" timestamp,
  "transaction_status" text,
  "remittance_status" text,
  "processed_date" timestamp,
  "amount" real
);

CREATE TABLE api.flowbird_HUB_transactions_raw (
  "id" bigint PRIMARY KEY,
  "payment_method" text,
  "meter_id" int,
  "timestamp" timestamp,
  "duration_min" real,
  "start_time" timestamp,
  "end_time" timestamp,
  "amount" real,
  "invoice_id" bigint,
  "validated" bool
);

CREATE TABLE api.passport_transactions_raw (
  "id" int PRIMARY KEY,
  "payment_method" text,
  "timestamp" timestamp,
  "amount" real
);

CREATE TABLE api.fiserv_reports_raw (
  "invoice_id" bigint PRIMARY KEY,
  "account" int,
  "transaction_date" timestamp,
  "transaction_type" text,
  "meter_id" int,
  "batch_number" bigint,
  "batch_sequence_number" bigint,
  "submit_date" timestamp,
  "funded_date" timestamp,
  "transaction_status" text,
  "validated" bool,
  "amount" real
);

CREATE TABLE api.validated_flowbird_transactions (
  "id" int PRIMARY KEY,
  "invoice_id" bigint REFERENCES api.fiserv_reports_raw ("invoice_id"),
  "payment_method" text,
  "meter_id" int,
  "timestamp" timestamp,
  "duration_min" int,
  "start_time" timestamp,
  "end_time" timestamp,
  "amount" real
);

CREATE TABLE api.not_validated_flowbird_transactions (
  "id" int PRIMARY KEY,
  "invoice_id" bigint,
  "payment_method" text,
  "meter_id" int,
  "timestamp" timestamp,
  "duration_min" int,
  "start_time" timestamp,
  "end_time" timestamp,
  "amount" real
);



--
-- Name: SCHEMA api; Type: ACL; Schema: -; Owner: postgres
--

GRANT USAGE ON SCHEMA api TO my_api_user;
GRANT USAGE ON SCHEMA api TO web_anon;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;
GRANT USAGE ON SCHEMA public TO my_api_user;


--
-- Name: TABLE knack; Type: ACL; Schema: api; Owner: postgres
--

GRANT ALL ON TABLE api.transactions TO my_api_user;
GRANT ALL ON TABLE api.flowbird_transactions_raw TO my_api_user;
GRANT ALL ON TABLE api.flowbird_HUB_transactions_raw TO my_api_user;
GRANT ALL ON TABLE api.passport_transactions_raw TO my_api_user;
GRANT ALL ON TABLE api.fiserv_reports_raw TO my_api_user;
GRANT ALL ON TABLE api.not_validated_flowbird_transactions TO my_api_user;
GRANT ALL ON TABLE api.validated_flowbird_transactions TO my_api_user;
GRANT ALL ON TABLE api.flowbird_payments_raw TO my_api_user;


--
-- PostgreSQL database dump complete
--