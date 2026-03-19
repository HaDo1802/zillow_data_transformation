select
    count(*)                        as total_rows,
    count(distinct zpid)            as unique_properties,
    count(distinct source_file)     as files_loaded,
    count(distinct zpid || source_file) as unique_combinations
from raw.raw_property_master_data;