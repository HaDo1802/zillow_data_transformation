CREATE SCHEMA IF NOT EXISTS raw;

-- 1) staging table: ALL TEXT (safe landing)
DROP TABLE IF EXISTS raw.raw_property_master_data_stg;

CREATE TABLE raw.raw_property_master_data_stg (
  address                TEXT,
  bathrooms              TEXT,
  bedrooms               TEXT,
  brokername             TEXT,
  carouselphotos         TEXT,
  comingsoononmarketdate TEXT,
  contingentlistingtype  TEXT,
  country                TEXT,
  currency               TEXT,
  datepricechanged       TEXT,
  daysonzillow           TEXT,
  detailurl              TEXT,
  has3dmodel             TEXT,
  hasimage               TEXT,
  hasvideo               TEXT,
  imgsrc                 TEXT,
  latitude               TEXT,
  listingstatus          TEXT,
  listingsubtype         TEXT,
  livingarea             TEXT,
  longitude              TEXT,
  lotareaunit            TEXT,
  lotareavalue           TEXT,
  price                  TEXT,
  pricechange            TEXT,
  propertytype           TEXT,
  rentzestimate          TEXT,
  variabledata           TEXT,
  zestimate              TEXT,
  zpid                   TEXT,
  unit                   TEXT,
  newconstructiontype    TEXT,
  extracted_at           TEXT,

  -- pipeline metadata (also text in staging)
  ingested_time          TEXT,
  snapshot_date          TEXT,
  source_file            TEXT
);

-- 2) typed raw table (your final destination)
DROP TABLE IF EXISTS raw.raw_property_master_data;

CREATE TABLE raw.raw_property_master_data (
  address                TEXT,
  bathrooms              NUMERIC(4,1),
  bedrooms               INTEGER,
  brokername             TEXT,
  carouselphotos         TEXT,
  comingsoononmarketdate TEXT,
  contingentlistingtype  TEXT,
  country                TEXT,
  currency               TEXT,
  datepricechanged       TEXT,
  daysonzillow           INTEGER,
  detailurl              TEXT,
  has3dmodel             BOOLEAN,
  hasimage               BOOLEAN,
  hasvideo               BOOLEAN,
  imgsrc                 TEXT,
  latitude               DOUBLE PRECISION,
  listingstatus          TEXT,
  listingsubtype         TEXT,
  livingarea             INTEGER,
  longitude              DOUBLE PRECISION,
  lotareaunit            TEXT,
  lotareavalue           DOUBLE PRECISION,
  price                  BIGINT,
  pricechange            BIGINT,
  propertytype           TEXT,
  rentzestimate          BIGINT,
  variabledata           TEXT,
  zestimate              BIGINT,
  zpid                   BIGINT NOT NULL,
  unit                   TEXT,
  newconstructiontype    TEXT,
  extracted_at           TIMESTAMPTZ NOT NULL,

  ingested_time          TIMESTAMPTZ NOT NULL DEFAULT now(),
  snapshot_date          TEXT,
  source_file            TEXT,

  CONSTRAINT raw_property_master_data_uniq
    UNIQUE (zpid, extracted_at, price)
);

CREATE INDEX IF NOT EXISTS idx_raw_pmd_zpid ON raw.raw_property_master_data (zpid);
CREATE INDEX IF NOT EXISTS idx_raw_pmd_extracted_at ON raw.raw_property_master_data (extracted_at);
