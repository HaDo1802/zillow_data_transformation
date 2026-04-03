create schema if not exists raw;

create table if not exists raw.raw_property_master_data (
    address             text,
    bathrooms           text,
    bedrooms            text,
    brokername          text,
    carouselphotos      text,
    comingsoononmarketdate text,
    contingentlistingtype text,
    country             text,
    currency            text,
    datepricechanged    text,
    daysonzillow        text,
    detailurl           text,
    has3dmodel          text,
    hasimage            text,
    hasvideo            text,
    imgsrc              text,
    latitude            text,
    listingstatus       text,
    listingsubtype      text,
    livingarea          text,
    longitude           text,
    lotareaunit         text,
    lotareavalue        text,
    price               text,
    pricechange         text,
    propertytype        text,
    rentzestimate       text,
    variabledata        text,
    zestimate           text,
    zpid                text,
    unit                text,
    newconstructiontype text,
    extracted_at        text,
    snapshot_date       text,
    ingested_at         text,
    source_file         text,

    constraint raw_property_master_data_uk
        unique (zpid, snapshot_date, price, listingstatus)
);

create unique index if not exists raw_property_master_data_uk_idx
    on raw.raw_property_master_data (zpid, snapshot_date, price, listingstatus);
