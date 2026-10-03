UPDATE settings
SET data = data - 'auto_dump_enabled' - 'auto_dump_storefronts'
WHERE id = 1
  AND (data ? 'auto_dump_enabled' OR data ? 'auto_dump_storefronts');
