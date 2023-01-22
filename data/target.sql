--
-- PostgreSQL database dump
--

DROP TABLE IF EXISTS order_detail;

--
-- Name: order_detail; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE order_detail (
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