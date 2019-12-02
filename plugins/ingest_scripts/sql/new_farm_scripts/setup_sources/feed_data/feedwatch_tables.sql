CREATE TABLE feedwatch_dmi (
    id                  serial primary key,
    farm_id             bigint,
    company_name        varchar(255),
    pen_name            varchar(255),
    report_date         date,
    avg_pen_count       float,
    total_dropped       float,
    avg_dropped         float,
    total_weighbacks    float,
    avg_weighbacks      float,
    total_dm            float,
    avg_dm_animal       float,
    UNIQUE (farm_id, pen_name, report_date)
);



CREATE TABLE feedwatch_ingredient (
    id                  serial primary key,
    farm_id             bigint,
    company_name        varchar(255),
    pen_type            varchar(255),
    ingredient_name     varchar(255),
    as_fed_quantity     float,
    as_fed_tons         float,
    report_date         date,
    UNIQUE (farm_id, pen_type, ingredient_name, report_date)
);