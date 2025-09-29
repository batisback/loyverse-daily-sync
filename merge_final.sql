MERGE `sbco_ops_jibble.final_jibble_attendance` T
USING (
  SELECT
    TO_HEX(SHA256(CONCAT(
      JSON_VALUE(payload, '$.Date'), '|',
      JSON_VALUE(payload, '$."Full Name"'), '|',
      JSON_VALUE(payload, '$.EntryType'), '|',
      JSON_VALUE(payload, '$.Time'), '|',
      IFNULL(JSON_VALUE(payload, '$."Kiosk Name"'),'')
    ))) AS attn_id,

    COALESCE(
      SAFE.PARSE_DATE('%F', JSON_VALUE(payload, '$.Date')),
      SAFE.PARSE_DATE('%d/%m/%Y', JSON_VALUE(payload, '$.Date'))
    ) AS date,

    JSON_VALUE(payload, '$."Full Name"') AS full_name,
    JSON_VALUE(payload, '$.Group') AS `group`,
    JSON_VALUE(payload, '$.EntryType') AS entry_type,

    `sbco_ops_jibble.to_ph_ts`(
      JSON_VALUE(payload, '$.Date'),
      JSON_VALUE(payload, '$.Time')
    ) AS time_ts,

    `sbco_ops_jibble.parse_hms_to_seconds`(JSON_VALUE(payload, '$.Duration')) AS duration_sec,

    JSON_VALUE(payload, '$.Activity') AS activity,
    JSON_VALUE(payload, '$."Kiosk Name"') AS kiosk_name,

    COALESCE(
      TIMESTAMP(JSON_VALUE(payload, '$."Last Edited On"')),
      TIMESTAMP(JSON_VALUE(payload, '$."Created On"')),
      load_dt
    ) AS updated_at

  FROM `sbco_ops_jibble.jibble_raw_attendance`
  WHERE payload IS NOT NULL
) S
ON T.attn_id = S.attn_id
WHEN MATCHED THEN UPDATE SET
  date = S.date,
  full_name = S.full_name,
  `group` = S.`group`,
  entry_type = S.entry_type,
  time_ts = S.time_ts,
  duration_sec = S.duration_sec,
  activity = S.activity,
  kiosk_name = S.kiosk_name,
  updated_at = S.updated_at
WHEN NOT MATCHED THEN
  INSERT ROW;
