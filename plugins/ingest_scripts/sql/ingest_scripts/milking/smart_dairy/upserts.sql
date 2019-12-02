# load incoming data into smart_dairy.tblcows

# upsert from tblcows to cows
INSERT INTO smart_dairy.tblcows (cow_id, cow_number, cow_name, breedcode_id, birth_date, lactations, isbull, iscomplete, active, tstamp)
    SELECT cow_id, cow_number, cow_name, breedcode_id, birth_date, lactations, isbull, iscomplete, active, tstamp
    FROM public.tblcows
ON CONFLICT (cow_id)
DO UPDATE
SET (cow_number, cow_name, breedcode_id, birth_date, lactations, isbull, iscomplete, active, tstamp) = (excluded.cow_number, excluded.cow_name, excluded.breedcode_id, excluded.birth_date, excluded.lactations, excluded.isbull, excluded.iscomplete, excluded.active, excluded.tstamp);

INSERT INTO smart_dairy.tblmilkings (
         milking_id, cow_id, milkingshift_id, milkingassignment_id, id_tag_number_assigned, id_tag_read_duration, identified_tstamp,
         id_control_address, milk_conductivity, lot_number_assigned, lot_number_milked, detacher_address, ismanualcownumber,
         milk_weight, cow_activity, flow_rate_to_detach, dumped_milk, mastitis_code, tstamp, max_milk_temperature)
  SELECT milking_id, cow_id, milkingshift_id, milkingassignment_id, id_tag_number_assigned, id_tag_read_duration, identified_tstamp,
         id_control_address, milk_conductivity, lot_number_assigned, lot_number_milked, detacher_address, ismanualcownumber,
         milk_weight, cow_activity, flow_rate_to_detach, dumped_milk, mastitis_code, tstamp, max_milk_temperature
    FROM public.tblmilkings
ON CONFLICT (milking_id)
DO UPDATE
SET (milkingshift_id, milkingassignment_id, id_tag_number_assigned, id_tag_read_duration, identified_tstamp,
     id_control_address, milk_conductivity, lot_number_assigned, lot_number_milked, detacher_address, ismanualcownumber,
     milk_weight, cow_activity, flow_rate_to_detach, dumped_milk, mastitis_code, tstamp, max_milk_temperature)
  = (excluded.milkingshift_id, excluded.milkingassignment_id, excluded.id_tag_number_assigned, excluded.id_tag_read_duration, excluded.identified_tstamp,
     excluded.id_control_address, excluded.milk_conductivity, excluded.lot_number_assigned, excluded.lot_number_milked, excluded.detacher_address, excluded.ismanualcownumber,
     excluded.milk_weight, excluded.cow_activity, excluded.flow_rate_to_detach, excluded.dumped_milk, excluded.mastitis_code, excluded.tstamp, excluded.max_milk_temperature);

INSERT INTO smart_dairy.tblattachtimes (attachtime_id, milking_id, detacher_address, tstamp)
    SELECT attachtime_id, milking_id, detacher_address, tstamp
    FROM public.tblattachtimes
ON CONFLICT (attachtime_id)
DO UPDATE
SET (milking_id, detacher_address, tstamp) = (excluded.milking_id, excluded.detacher_address, excluded.tstamp);

INSERT INTO smart_dairy.tbldetachtimes (detachtime_id, milking_id, detacher_address, milk_weight, tstamp)
    SELECT detachtime_id, milking_id, detacher_address, milk_weight, tstamp
    FROM public.tbldetachtimes
ON CONFLICT (detachtime_id)
DO UPDATE
SET (milking_id, detacher_address, milk_weight, tstamp) = (excluded.milking_id, excluded.detacher_address, excluded.milk_weight, excluded.tstamp);

INSERT INTO smart_dairy.tblcowtags (cowtag_id, cow_id, tagtype_id, tag_number, is_management, active, tstamp)
    SELECT cowtag_id, cow_id, tagtype_id, tag_number, is_management, active, tstamp
    FROM public.tblcowtags
ON CONFLICT (cowtag_id)
DO UPDATE
SET (cow_id, tagtype_id, tag_number, is_management, active, tstamp) = (excluded.cow_id, excluded.tagtype_id, excluded.tag_number, excluded.is_management, excluded.active, excluded.tstamp);

INSERT INTO smart_dairy.tblcowlots (cowlot_id, cow_id, lot_id, active, tstamp)
    SELECT cowlot_id, cow_id, lot_id, active, tstamp
    FROM public.tblcowlots
ON CONFLICT (cowlot_id)
DO UPDATE
SET (cowlot_id, cow_id, lot_id, active, tstamp) = (excluded.cowlot_id, excluded.cow_id, excluded.lot_id, excluded.active, excluded.tstamp);

INSERT INTO smart_dairy.tblcowreprostatuses (cowreprostatus_id, cow_id, reprostatus_id, active, tstamp)
    SELECT cowreprostatus_id, cow_id, reprostatus_id, active, tstamp
    FROM public.tblcowreprostatuses
ON CONFLICT (cowreprostatus_id)
DO UPDATE
SET (cowreprostatus_id, cow_id, reprostatus_id, active, tstamp) = (excluded.cowreprostatus_id, excluded.cow_id, excluded.reprostatus_id, excluded.active, excluded.tstamp);
