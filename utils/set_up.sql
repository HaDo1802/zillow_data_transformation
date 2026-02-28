ALTER TABLE gold.fact_property_snapshot
ADD CONSTRAINT fk_property
FOREIGN KEY (property_id) 
REFERENCES gold.dim_property(property_id);

ALTER TABLE gold.fact_property_snapshot
ADD CONSTRAINT fk_date
FOREIGN KEY (snapshot_date) 
REFERENCES gold.dim_date(date_day);

-- 1. Make property_id the Primary Key for dim_property
ALTER TABLE gold.dim_property 
ADD PRIMARY KEY (property_id);


-- 2. Make date_day the Primary Key for dim_date
ALTER TABLE gold.dim_date 
ADD PRIMARY KEY (date_day);

ALTER TABLE gold.fact_property_snapshot 
ADD PRIMARY KEY (property_id, snapshot_date);