-- Correct only the flagged public seed from 20261015_nasdaq100_seed.
-- The last documented constituent change took effect on 2026-10-09.
-- Preserve incomplete verification metadata and other membership history.
UPDATE qd_universe_members m
SET valid_from = DATE '2026-10-09'
FROM qd_universes u
WHERE m.universe_id = u.id
  AND u.code = 'nasdaq100' AND u.is_system = TRUE
  AND m.source_version = 'flagged-public-20261014'
  AND m.metadata_json->>'as_of' = '2026-10-14'
  AND m.metadata_json->>'verification' = 'flagged_incomplete_public_list'
  AND m.metadata_json->>'source' = 'https://stockti.com/stocks/nasdaq-100'
  AND m.metadata_json->>'change_source' = 'https://nasdaqinc.gcs-web.com/news-releases/news-release-details/moderna-inc-join-nasdaq-100-indexr-beginning-october-9-2026'
  AND m.valid_from = DATE '2026-10-14';
