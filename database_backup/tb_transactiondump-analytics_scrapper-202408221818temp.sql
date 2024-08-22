--
-- PostgreSQL database dump
--

-- Dumped from database version 14.13 (Ubuntu 14.13-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 16.1

-- Started on 2024-08-22 18:18:36

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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 220 (class 1259 OID 185865)
-- Name: tb_transaction; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.tb_transaction (
    transaction_id character varying(20) NOT NULL,
    transaction_channel character varying(20) NOT NULL,
    model_product character varying(255) NOT NULL,
    price_product character varying(255) NOT NULL,
    no_hp_cust character varying(255) NOT NULL,
    name_cust character varying(255) NOT NULL,
    city_cust character varying(255),
    prov_cust character varying(255) NOT NULL,
    address_cust character varying(255) NOT NULL,
    instagram_cust character varying(255),
    created_by character varying(255) NOT NULL,
    created_dt timestamp(0) without time zone NOT NULL,
    updated_by character varying(255) NOT NULL,
    updated_dt timestamp(0) without time zone NOT NULL,
    transaction_dt date NOT NULL
);


ALTER TABLE public.tb_transaction OWNER TO postgres;

--
-- TOC entry 3349 (class 0 OID 0)
-- Dependencies: 220
-- Name: COLUMN tb_transaction.transaction_channel; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tb_transaction.transaction_channel IS 'offline/online';


--
-- TOC entry 3343 (class 0 OID 185865)
-- Dependencies: 220
-- Data for Name: tb_transaction; Type: TABLE DATA; Schema: public; Owner: postgres
--

INSERT INTO public.tb_transaction VALUES ('TT0001210824ON', 'Online', 'CBR', '7500000', '081234567890', 'John Doe', 'Jakarta', 'DKI Jakarta', 'Jl. Sudirman No. 10', 'johndoe_insta', 'admin', '2024-08-21 00:00:00', 'admin', '2024-08-21 00:00:00', '2024-08-21');
INSERT INTO public.tb_transaction VALUES ('TT0002210824OF', 'Offline', 'CB', '12500000', '082345678901', 'Jane Smith', 'Bandung', 'Jawa Barat', 'Jl. Setiabudi No. 15', 'janesmith_insta', 'admin', '2024-08-21 00:00:00', 'admin', '2024-08-21 00:00:00', '2024-08-21');
INSERT INTO public.tb_transaction VALUES ('TT0003210824ON', 'Online', 'CBR PUTIH', '5000000', '083456789012', 'Alice Johnson', 'Surabaya', 'Jawa Timur', 'Jl. Darmo No. 20', 'alicejohnson_insta', 'admin', '2024-08-21 00:00:00', 'admin', '2024-08-21 00:00:00', '2024-08-21');
INSERT INTO public.tb_transaction VALUES ('TT0004210824ON', 'Online', 'CBR PUTIH', '5000000', '083456789012', 'Alice Johnson', 'Surabaya', 'Jawa Timur', 'Jl. Darmo No. 20', 'alicejohnson_insta', 'admin', '2024-08-21 00:00:00', 'admin', '2024-08-21 00:00:00', '2024-08-21');
INSERT INTO public.tb_transaction VALUES ('TT0008150824ON', 'Online', 'OBI', '123234', '23424', '234543', '435234', 'JAMBI', '324234324 23', '32 24 23', '', '2024-08-22 00:00:00', '', '2024-08-22 00:00:00', '2024-08-15');
INSERT INTO public.tb_transaction VALUES ('TT0009220824OF', 'Offline', 'ROPE', '234234234', '0812332423', 'ras sdfsf', 'cimahi', 'JAWA TENGAH', 'jl amirudin', 'amirudin123@', 'SYSTEM', '2024-08-22 00:00:00', 'SYSTEM', '2024-08-22 00:00:00', '2024-08-22');
INSERT INTO public.tb_transaction VALUES ('TT0010220824OF', 'Offline', 'ROPE', '234234234', '0812332423', 'ras sdfsf', 'cimahi', 'JAWA TENGAH', 'jl amirudin', 'amirudin123@', 'SYSTEM', '2024-08-22 00:00:00', 'SYSTEM', '2024-08-22 00:00:00', '2024-08-22');
INSERT INTO public.tb_transaction VALUES ('TT0011220824OF', 'Offline', 'VEST', '2342435', '085343435434', 'rasga', 'Samarinda', 'YOGYAKARTA', 'Jl. Tset 12345', 'pororo123@', 'SYSTEM', '2024-08-22 00:00:00', 'SYSTEM', '2024-08-22 00:00:00', '2024-08-22');
INSERT INTO public.tb_transaction VALUES ('TT0012220824ON', 'Online', 'CBR', '4235345', '3435353453', 'asdf ghjh', 'asdf gad', 'PAPUA', 'Jl.gelatik 1 No 4', 'asdf#', 'SYSTEM', '2024-08-22 00:00:00', 'SYSTEM', '2024-08-22 00:00:00', '2024-08-22');
INSERT INTO public.tb_transaction VALUES ('TT0013220824ON', 'Online', 'CBR', '23324234', '23424322342234', 'adsf asd as', 'asfd fd', 'JAWA TIMUR', 'adf 123', 'asdfsadf', 'SYSTEM', '2024-08-22 09:32:32', 'SYSTEM', '2024-08-22 09:32:32', '2024-08-22');
INSERT INTO public.tb_transaction VALUES ('TT0017210824OF', 'Offline', 'OBI', '32452345234523522', '3243243245245', 'adbbaru', 'afsdsadf baru', 'SUMATERA BARAT', 'asdfsafdbaru ', '4325sfa@2baru', 'SYSTEM', '2024-08-22 10:49:15', 'SYSTEM', '2024-08-22 10:49:15', '2024-08-21');
INSERT INTO public.tb_transaction VALUES ('TT0018210824OF', 'Offline', 'OBI', '32452345234523522', '3243243245245', 'adbbaru', 'afsdsadf baru', 'SUMATERA BARAT', 'asdfsafdbaru ', '4325sfa@2baru', 'SYSTEM', '2024-08-22 10:49:15', 'SYSTEM', '2024-08-22 10:49:15', '2024-08-21');


--
-- TOC entry 3203 (class 2606 OID 185871)
-- Name: tb_transaction tb_transaction_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tb_transaction
    ADD CONSTRAINT tb_transaction_pkey PRIMARY KEY (transaction_id);


-- Completed on 2024-08-22 18:18:38

--
-- PostgreSQL database dump complete
--

