--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;


SET default_tablespace = '';

SET default_with_oids = false;


DROP TABLE IF EXISTS order_detail;

--
-- Name: order_detail; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE order_detail
(
    order_id smallint NOT NULL,
    product_id smallint NOT NULL,
    unit_price real,
    quantity character varying(20),
    discount real,
    product_name character varying(40) NOT NULL
);

--
-- PostgreSQL database dump complete
--