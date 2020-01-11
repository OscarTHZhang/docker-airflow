--
-- PostgreSQL database dump
--

-- Dumped from database version 12.1
-- Dumped by pg_dump version 12.1

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
-- Name: farm_datasource; Type: TABLE; Schema: public; Owner: dairybrain
--

CREATE TABLE public.farm_datasource (
    id integer NOT NULL,
    farm_id integer,
    software_id integer,
    software_version text,
    file_location text,
    datasource_type text,
    script_name text,
    script_arguments text,
    active boolean
);


ALTER TABLE public.farm_datasource OWNER TO dairybrain;

--
-- Name: farm_datasource_id_seq; Type: SEQUENCE; Schema: public; Owner: dairybrain
--

CREATE SEQUENCE public.farm_datasource_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.farm_datasource_id_seq OWNER TO dairybrain;

--
-- Name: farm_datasource_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dairybrain
--

ALTER SEQUENCE public.farm_datasource_id_seq OWNED BY public.farm_datasource.id;


--
-- Name: farm_ingest_status; Type: TABLE; Schema: public; Owner: dairybrain
--

CREATE TABLE public.farm_ingest_status (
    farm_id integer,
    active boolean DEFAULT false NOT NULL
);


ALTER TABLE public.farm_ingest_status OWNER TO dairybrain;

--
-- Name: farms; Type: TABLE; Schema: public; Owner: dairybrain
--

CREATE TABLE public.farms (
    farm_id integer NOT NULL,
    name character varying(50) NOT NULL,
    owner text NOT NULL,
    address text,
    phone character varying(20),
    create_date date DEFAULT CURRENT_DATE NOT NULL,
    dialect text DEFAULT 'postgres'::text,
    ingest_user text,
    host text DEFAULT 'dairy-db-2.discovery.wisc.edu'::text,
    port integer DEFAULT 5432,
    database text
);


ALTER TABLE public.farms OWNER TO dairybrain;

--
-- Name: farms_farm_id_seq; Type: SEQUENCE; Schema: public; Owner: dairybrain
--

CREATE SEQUENCE public.farms_farm_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.farms_farm_id_seq OWNER TO dairybrain;

--
-- Name: farms_farm_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dairybrain
--

ALTER SEQUENCE public.farms_farm_id_seq OWNED BY public.farms.farm_id;


--
-- Name: ingest_log; Type: TABLE; Schema: public; Owner: dairybrain
--

CREATE TABLE public.ingest_log (
    id integer NOT NULL,
    farm_id integer,
    farm_datasource_id integer,
    run_date date DEFAULT CURRENT_DATE,
    ingest_file_name text,
    export_date date,
    message text,
    success boolean
);


ALTER TABLE public.ingest_log OWNER TO dairybrain;

--
-- Name: ingest_log_id_seq; Type: SEQUENCE; Schema: public; Owner: dairybrain
--

CREATE SEQUENCE public.ingest_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ingest_log_id_seq OWNER TO dairybrain;

--
-- Name: ingest_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dairybrain
--

ALTER SEQUENCE public.ingest_log_id_seq OWNED BY public.ingest_log.id;


--
-- Name: software; Type: TABLE; Schema: public; Owner: dairybrain
--

CREATE TABLE public.software (
    id integer NOT NULL,
    name text,
    vendor text
);


ALTER TABLE public.software OWNER TO dairybrain;

--
-- Name: software_id_seq; Type: SEQUENCE; Schema: public; Owner: dairybrain
--

CREATE SEQUENCE public.software_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.software_id_seq OWNER TO dairybrain;

--
-- Name: software_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dairybrain
--

ALTER SEQUENCE public.software_id_seq OWNED BY public.software.id;


--
-- Name: farm_datasource id; Type: DEFAULT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.farm_datasource ALTER COLUMN id SET DEFAULT nextval('public.farm_datasource_id_seq'::regclass);


--
-- Name: farms farm_id; Type: DEFAULT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.farms ALTER COLUMN farm_id SET DEFAULT nextval('public.farms_farm_id_seq'::regclass);


--
-- Name: ingest_log id; Type: DEFAULT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.ingest_log ALTER COLUMN id SET DEFAULT nextval('public.ingest_log_id_seq'::regclass);


--
-- Name: software id; Type: DEFAULT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.software ALTER COLUMN id SET DEFAULT nextval('public.software_id_seq'::regclass);


--
-- Data for Name: farm_datasource; Type: TABLE DATA; Schema: public; Owner: dairybrain
--

COPY public.farm_datasource (id, farm_id, software_id, software_version, file_location, datasource_type, script_name, script_arguments, active) FROM stdin;
2	1	1	\N	/usr/local/airflow/test/larson/feedwatch	feed	feed_data_ingest.py	\N	t
8	6	\N	\N	/usr/local/airflow/test/arlington/dairycomp	event	event_data_ingest.py	\N	t
3	1	1	\N	/usr/local/airflow/test/temp_farm/feedwatch	feed	feed_data_ingest.py	\N	t
7	6	\N	\N	/usr/local/airflow/test/arlington/agsource	agsource	agsource_data_ingest.py	\N	t
5	3	\N	\N	/usr/local/airflow/test/mystic_valley/dairycomp	event	event_data_ingest.py		t
6	3	\N	\N	/usr/local/airflow/test/mystic_valley/tmrtracker	feed	feed_data_ingest.py		t
4	2	1		/usr/local/airflow/test/wangen/dairycomp	event	event_data_ingest.py		f
\.


--
-- Data for Name: farm_ingest_status; Type: TABLE DATA; Schema: public; Owner: dairybrain
--

COPY public.farm_ingest_status (farm_id, active) FROM stdin;
1	t
2	f
3	t
6	t
\.


--
-- Data for Name: farms; Type: TABLE DATA; Schema: public; Owner: dairybrain
--

COPY public.farms (farm_id, name, owner, address, phone, create_date, dialect, ingest_user, host, port, database) FROM stdin;
1	Larson Acres	Ed Larson			2018-07-26	postgres	ingest_user	dairy-db-2.discovery.wisc.edu	5432	larson
2	Wangen Farms	Steve Wangen			2018-07-26	postgres	ingest_user	dairy-db-2.discovery.wisc.edu	5432	wangen
3	Mystic Valley Dairy	Mitch Breunig			2018-07-26	postgres	ingest_user	dairy-db-2.discovery.wisc.edu	5432	mystic
5	Hazy Farms	Jeff Dischler	1234 Hazy Ln	(608) 320-0459	2019-01-07	postgres	\N	dairy-db-2.discovery.wisc.edu	5432	\N
6	Arlington	University of Wisconsin - Madison			2019-01-22	postgres	\N	dairy-db-2.discovery.wisc.edu	5432	arlington
\.


--
-- Data for Name: ingest_log; Type: TABLE DATA; Schema: public; Owner: dairybrain
--

COPY public.ingest_log (id, farm_id, farm_datasource_id, run_date, ingest_file_name, export_date, message, success) FROM stdin;
\.


--
-- Data for Name: software; Type: TABLE DATA; Schema: public; Owner: dairybrain
--

COPY public.software (id, name, vendor) FROM stdin;
1	DairyCOMP 305	Valley Ag Software
2	FeedWatch	DairyOne
\.


--
-- Name: farm_datasource_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dairybrain
--

SELECT pg_catalog.setval('public.farm_datasource_id_seq', 7, true);


--
-- Name: farms_farm_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dairybrain
--

SELECT pg_catalog.setval('public.farms_farm_id_seq', 6, true);


--
-- Name: ingest_log_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dairybrain
--

SELECT pg_catalog.setval('public.ingest_log_id_seq', 1, false);


--
-- Name: software_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dairybrain
--

SELECT pg_catalog.setval('public.software_id_seq', 2, true);


--
-- Name: farm_datasource farm_datasource_pkey; Type: CONSTRAINT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.farm_datasource
    ADD CONSTRAINT farm_datasource_pkey PRIMARY KEY (id);


--
-- Name: farms farms_pkey; Type: CONSTRAINT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.farms
    ADD CONSTRAINT farms_pkey PRIMARY KEY (farm_id);


--
-- Name: ingest_log ingest_log_pkey; Type: CONSTRAINT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.ingest_log
    ADD CONSTRAINT ingest_log_pkey PRIMARY KEY (id);


--
-- Name: software software_pkey; Type: CONSTRAINT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.software
    ADD CONSTRAINT software_pkey PRIMARY KEY (id);


--
-- Name: farm_datasource farm_datasource_farm_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.farm_datasource
    ADD CONSTRAINT farm_datasource_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farms(farm_id);


--
-- Name: farm_datasource farm_datasource_software_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.farm_datasource
    ADD CONSTRAINT farm_datasource_software_id_fkey FOREIGN KEY (software_id) REFERENCES public.software(id);


--
-- Name: farm_ingest_status farm_ingest_status_farm_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.farm_ingest_status
    ADD CONSTRAINT farm_ingest_status_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farms(farm_id);


--
-- Name: ingest_log ingest_log_farm_datasource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.ingest_log
    ADD CONSTRAINT ingest_log_farm_datasource_id_fkey FOREIGN KEY (farm_datasource_id) REFERENCES public.farm_datasource(id);


--
-- Name: ingest_log ingest_log_farm_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dairybrain
--

ALTER TABLE ONLY public.ingest_log
    ADD CONSTRAINT ingest_log_farm_id_fkey FOREIGN KEY (farm_id) REFERENCES public.farms(farm_id);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: postgres
--

GRANT USAGE ON SCHEMA public TO ingest;


--
-- Name: TABLE farm_datasource; Type: ACL; Schema: public; Owner: dairybrain
--

GRANT SELECT ON TABLE public.farm_datasource TO ingest;


--
-- Name: TABLE farm_ingest_status; Type: ACL; Schema: public; Owner: dairybrain
--

GRANT SELECT ON TABLE public.farm_ingest_status TO ingest;


--
-- Name: TABLE farms; Type: ACL; Schema: public; Owner: dairybrain
--

GRANT SELECT,INSERT,UPDATE ON TABLE public.farms TO ingest;


--
-- Name: SEQUENCE farms_farm_id_seq; Type: ACL; Schema: public; Owner: dairybrain
--

GRANT USAGE ON SEQUENCE public.farms_farm_id_seq TO ingest;


--
-- Name: TABLE ingest_log; Type: ACL; Schema: public; Owner: dairybrain
--

GRANT SELECT ON TABLE public.ingest_log TO ingest;


--
-- Name: TABLE software; Type: ACL; Schema: public; Owner: dairybrain
--

GRANT SELECT ON TABLE public.software TO ingest;


--
-- PostgreSQL database dump complete
--

