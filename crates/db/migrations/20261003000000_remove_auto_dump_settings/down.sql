UPDATE settings
SET data = data || jsonb_build_object(
    'auto_dump_enabled', true,
    'auto_dump_storefronts', jsonb_build_array('us')
)
WHERE id = 1;
