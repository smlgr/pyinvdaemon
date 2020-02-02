CREATE TABLE data
(
    id         bigserial                              NOT NULL
        CONSTRAINT data_pk PRIMARY KEY,
    ts         timestamp with time zone DEFAULT now() NOT NULL,
    dc_voltage float                                  NOT NULL,
    dc_current float                                  NOT NULL,
    ac_voltage float                                  NOT NULL,
    ac_current float                                  NOT NULL,
    power      float                                  NOT NULL,
    frequency  float                                  NOT NULL
);

CREATE UNIQUE index data_ts_uindex ON DATA (ts);
