DROP SCHEMA IF EXISTS public CASCADE;

CREATE SCHEMA public;

CREATE TABLE public.document (
    id bigint NOT NULL
);

CREATE TABLE public.t1 (
    document bigint NOT NULL,
    id bigint NOT NULL
);

CREATE TABLE public.t2 (
    t1 bigint,
    id bigint NOT NULL
);

CREATE TABLE public.u (
    id bigint NOT NULL,
    data text NOT NULL,
    t1 bigint NOT NULL,
    t2 bigint NOT NULL
);

ALTER TABLE ONLY public.document
    ADD CONSTRAINT document_primary PRIMARY KEY (id);

ALTER TABLE ONLY public.t1
    ADD CONSTRAINT t1_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.t2
    ADD CONSTRAINT t2_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.u
    ADD CONSTRAINT u_pkey PRIMARY KEY (id);

CREATE INDEX fki_t1_document ON public.t1 USING btree (document);

CREATE INDEX fki_t2_t1 ON public.t2 USING btree (t1);

CREATE INDEX fki_u_t1 ON public.u USING btree (t1);

CREATE INDEX fki_u_t2 ON public.u USING btree (t2);

ALTER TABLE ONLY public.t1
    ADD CONSTRAINT t1_document FOREIGN KEY (document) REFERENCES public.document(id) ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE ONLY public.t2
    ADD CONSTRAINT t2_t1 FOREIGN KEY (t1) REFERENCES public.t1(id) ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE ONLY public.u
    ADD CONSTRAINT u_t1 FOREIGN KEY (t1) REFERENCES public.t1(id) ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE ONLY public.u
    ADD CONSTRAINT u_t2 FOREIGN KEY (t2) REFERENCES public.t2(id) ON UPDATE RESTRICT ON DELETE CASCADE;
