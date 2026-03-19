select zpid, source_file, count(*)
from raw.raw_property_master_data
group by zpid, source_file
having count(*) > 1;