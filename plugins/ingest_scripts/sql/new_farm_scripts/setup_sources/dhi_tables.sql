DROP SCHEMA IF EXISTS ag_source CASCADE;
CREATE SCHEMA ag_source;

drop table if exists ag_source.components CASCADE;
CREATE TABLE ag_source.components (
    id                  serial primary key,
    farm_id             bigint,
    sample_date         date,
    cntl                int,
    bname               int,
    sample_id           int,
    dim                 int,
    milk                int,
    fat                 float,
    true_prot           float,
    lactose             float,
    snf                 float,
    cells               int,
    urea                float,
    mono_unsaturated_fa float,
    poly_unsaturated_fa float,
    saturated_fa        float,
    tot_unsaturated_fat float,
    scfa                float,
    mcfa                float,
    lcfa                float,
    trans               float,
    aceton              float,
    bhb                 float,
    C14_0               float,
    C16_0               float,
    C18_0               float,
    C18_1               float,
    de_novo_fa          float,
    mixed_fa            float,
    preformed_fa        float,
    sample_order        int,
    UNIQUE(farm_id, sample_id, sample_date)
);




drop table if exists ag_source.spectrum CASCADE;
CREATE TABLE ag_source.spectrum (
    id                  serial primary key,
    farm_id             bigint,
    sample_date         timestamp,
    t_stamp             timestamp,
    job_type_name       varchar(255),
    sample_id           int,
    wave_number         int,
    min_mu              int,
    max_mu              int,
    values              float[1060],
    UNIQUE(farm_id, sample_date, sample_id)
);



drop table if exists ag_source.ketosis_results;
CREATE TABLE ag_source.ketosis_results (
    id                  serial primary key,
    component_id        bigint references ag_source.components(id),
    spectrum_id         bigint references ag_source.spectrum(id),
    bhb_predict         float,
    ketosis             boolean default False
);



GRANT USAGE
    ON SCHEMA ag_source
    TO ingest;

GRANT CREATE
    ON SCHEMA ag_source
    TO ingest;

GRANT USAGE, SELECT, UPDATE
    ON SEQUENCE ag_source.components_id_seq
    TO ingest;

GRANT USAGE, SELECT, UPDATE
    ON SEQUENCE ag_source.spectrum_id_seq
    TO ingest;

GRANT SELECT, UPDATE, INSERT
    ON TABLE ag_source.components
    TO ingest;

GRANT SELECT, UPDATE, INSERT
    ON TABLE ag_source.spectrum
    TO ingest;