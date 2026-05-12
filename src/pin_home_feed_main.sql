@{OPTIMIZER_VERSION=8}
WITH
  Params AS (
  SELECT
    CAST([] AS ARRAY<STRUCT<id STRING, priority INT64>>) AS p_keymap_order,
    '' AS p_sort_field,
    '' AS p_sort_field_value,
    CAST(NULL AS FLOAT64) AS p_last_score,
    CAST(NULL AS TIMESTAMP) AS p_last_publish_date,
    '' AS p_last_id,
    10 AS p_limit,  -- ⚠️  To change: Edit LIMIT at bottom of SQL directly,
    CAST([] AS ARRAY<STRING>) AS p_id_list,
    'th' AS p_language,
    '' AS p_create_by_ssoid,
    CAST([] AS ARRAY<STRING>) AS p_content_type,
    CAST([] AS ARRAY<STRING>) AS p_exclude_ids
  ),
-- ============================================================
-- BASE DATA + FILTERS
-- ============================================================
BaseDataRaw AS (
		SELECT
			m.id, m.publish_date,
			0 AS hit_7,
			0.0 AS search_score,
			0.0 AS similarity_score,
			(SELECT
      CASE
        WHEN x IN ('badminton', 'tennis', 'f1', 'nba', 'ufc', 'volleyball') THEN 'sports'
        ELSE x
      END
     FROM UNNEST(m.article_category) AS x
     WHERE x IS NOT NULL
     LIMIT 1
    ) AS agg_group_key,
			0.0 AS combined_score,
			COALESCE(
      (SELECT priority FROM UNNEST((SELECT p_keymap_order FROM Params)) WHERE id = m.id),
      999999
    ) AS keymap_priority,
			
			ABS(FARM_FINGERPRINT(m.id)) AS random_seed
		  FROM mst_sfv_nonprod m
		WHERE
		    m.status = 'publish'
    AND m.publish_date <= CURRENT_TIMESTAMP()
    AND m.searchable = 'Y'
    AND (m.expire_date IS NULL OR m.expire_date > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE))
    AND m.title != 'empty title'
    AND (ARRAY_LENGTH((SELECT p_id_list FROM Params)) = 0 OR m.id IN UNNEST((SELECT p_id_list FROM Params)))
    AND m.lang = IFNULL((SELECT p_language FROM Params), 'th')
    AND ((SELECT p_create_by_ssoid FROM Params) IS NULL OR (SELECT p_create_by_ssoid FROM Params) = '' OR m.create_by_ssoid = (SELECT p_create_by_ssoid FROM Params))
    AND (ARRAY_LENGTH((SELECT p_content_type FROM Params)) = 0 OR m.content_type IN UNNEST((SELECT p_content_type FROM Params)))
    AND (ARRAY_LENGTH((SELECT p_exclude_ids FROM Params)) = 0 OR m.id NOT IN UNNEST((SELECT p_exclude_ids FROM Params)))
    AND (m.publish_date <= CURRENT_TIMESTAMP())
		
 UNION ALL 
		SELECT
			m.id, m.publish_date,
			0 AS hit_7,
			0.0 AS search_score,
			0.0 AS similarity_score,
			(SELECT
      CASE
        WHEN x IN ('badminton', 'tennis', 'f1', 'nba', 'ufc', 'volleyball') THEN 'sports'
        ELSE x
      END
     FROM UNNEST(m.article_category) AS x
     WHERE x IS NOT NULL
     LIMIT 1
    ) AS agg_group_key,
			0.0 AS combined_score,
			COALESCE(
      (SELECT priority FROM UNNEST((SELECT p_keymap_order FROM Params)) WHERE id = m.id),
      999999
    ) AS keymap_priority,
			
			ABS(FARM_FINGERPRINT(m.id)) AS random_seed
		  FROM mst_sfvseries_nonprod m
		WHERE
		    m.status = 'publish'
    AND m.publish_date <= CURRENT_TIMESTAMP()
    AND m.searchable = 'Y'
    AND (m.expire_date IS NULL OR m.expire_date > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE))
    AND m.title != 'empty title'
    AND (ARRAY_LENGTH((SELECT p_id_list FROM Params)) = 0 OR m.id IN UNNEST((SELECT p_id_list FROM Params)))
    AND m.lang = IFNULL((SELECT p_language FROM Params), 'th')
    AND ((SELECT p_create_by_ssoid FROM Params) IS NULL OR (SELECT p_create_by_ssoid FROM Params) = '' OR m.create_by_ssoid = (SELECT p_create_by_ssoid FROM Params))
    AND (ARRAY_LENGTH((SELECT p_content_type FROM Params)) = 0 OR m.content_type IN UNNEST((SELECT p_content_type FROM Params)))
    AND (ARRAY_LENGTH((SELECT p_exclude_ids FROM Params)) = 0 OR m.id NOT IN UNNEST((SELECT p_exclude_ids FROM Params)))
    AND (m.publish_date <= CURRENT_TIMESTAMP())
		
 UNION ALL 
		SELECT
			m.id, m.publish_date,
			0 AS hit_7,
			0.0 AS search_score,
			0.0 AS similarity_score,
			(SELECT
      CASE
        WHEN x IN ('badminton', 'tennis', 'f1', 'nba', 'ufc', 'volleyball') THEN 'sports'
        ELSE x
      END
     FROM UNNEST(m.article_category) AS x
     WHERE x IS NOT NULL
     LIMIT 1
    ) AS agg_group_key,
			0.0 AS combined_score,
			COALESCE(
      (SELECT priority FROM UNNEST((SELECT p_keymap_order FROM Params)) WHERE id = m.id),
      999999
    ) AS keymap_priority,
			
			ABS(FARM_FINGERPRINT(m.id)) AS random_seed
		  FROM mst_misc_nonprod m
		WHERE
		    m.status = 'publish'
    AND m.publish_date <= CURRENT_TIMESTAMP()
    AND m.searchable = 'Y'
    AND (m.expire_date IS NULL OR m.expire_date > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE))
    AND m.title != 'empty title'
    AND (ARRAY_LENGTH((SELECT p_id_list FROM Params)) = 0 OR m.id IN UNNEST((SELECT p_id_list FROM Params)))
    AND m.lang = IFNULL((SELECT p_language FROM Params), 'th')
    AND ((SELECT p_create_by_ssoid FROM Params) IS NULL OR (SELECT p_create_by_ssoid FROM Params) = '' OR m.create_by_ssoid = (SELECT p_create_by_ssoid FROM Params))
    AND (ARRAY_LENGTH((SELECT p_content_type FROM Params)) = 0 OR m.content_type IN UNNEST((SELECT p_content_type FROM Params)))
    AND (ARRAY_LENGTH((SELECT p_exclude_ids FROM Params)) = 0 OR m.id NOT IN UNNEST((SELECT p_exclude_ids FROM Params)))
    AND (m.publish_date <= CURRENT_TIMESTAMP())
		
 UNION ALL 
		SELECT
			m.id, m.publish_date,
			0 AS hit_7,
			0.0 AS search_score,
			0.0 AS similarity_score,
			(SELECT
      CASE
        WHEN x IN ('badminton', 'tennis', 'f1', 'nba', 'ufc', 'volleyball') THEN 'sports'
        ELSE x
      END
     FROM UNNEST(m.article_category) AS x
     WHERE x IS NOT NULL
     LIMIT 1
    ) AS agg_group_key,
			0.0 AS combined_score,
			COALESCE(
      (SELECT priority FROM UNNEST((SELECT p_keymap_order FROM Params)) WHERE id = m.id),
      999999
    ) AS keymap_priority,
			
			ABS(FARM_FINGERPRINT(m.id)) AS random_seed
		  FROM mst_ugcsfv_nonprod m
		WHERE
		    m.status = 'publish'
    AND m.publish_date <= CURRENT_TIMESTAMP()
    AND m.searchable = 'Y'
    AND m.title != 'empty title'
    AND (ARRAY_LENGTH((SELECT p_id_list FROM Params)) = 0 OR m.id IN UNNEST((SELECT p_id_list FROM Params)))
    AND m.lang = IFNULL((SELECT p_language FROM Params), 'th')
    AND ((SELECT p_create_by_ssoid FROM Params) IS NULL OR (SELECT p_create_by_ssoid FROM Params) = '' OR m.create_by_ssoid = (SELECT p_create_by_ssoid FROM Params))
    AND (ARRAY_LENGTH((SELECT p_content_type FROM Params)) = 0 OR m.content_type IN UNNEST((SELECT p_content_type FROM Params)))
    AND (ARRAY_LENGTH((SELECT p_exclude_ids FROM Params)) = 0 OR m.id NOT IN UNNEST((SELECT p_exclude_ids FROM Params)))
    AND (m.publish_date <= CURRENT_TIMESTAMP())
		
),
-- ============================================================
-- KEYSET PAGINATION
-- ============================================================
BaseData AS (
			SELECT * FROM BaseDataRaw
			WHERE
				(SELECT p_last_publish_date FROM Params) IS NULL
				OR (
				combined_score < (SELECT p_last_score FROM Params)
				OR (
					combined_score = (SELECT p_last_score FROM Params)
					AND (
					publish_date < (SELECT p_last_publish_date FROM Params)
					OR (publish_date = (SELECT p_last_publish_date FROM Params) AND id < (SELECT p_last_id FROM Params))
					)
				)
				)
			)


-- ============================================================
-- FINAL SELECT
-- ============================================================
SELECT
			id, publish_date,
			'MAIN_LIST' AS result_type,
			CAST(0 AS INT64) AS agg_rank
		FROM BaseData
		ORDER BY
		    CASE WHEN ARRAY_LENGTH((SELECT p_keymap_order FROM Params)) > 0 THEN keymap_priority END ASC,
    CASE WHEN (SELECT p_sort_field FROM Params) = 'publish_date' AND (SELECT p_sort_field_value FROM Params) = 'desc' THEN UNIX_MICROS(publish_date) END DESC,
    CASE WHEN (SELECT p_sort_field FROM Params) = 'publish_date' AND (SELECT p_sort_field_value FROM Params) = 'asc'  THEN UNIX_MICROS(publish_date) END ASC,
    CASE WHEN (SELECT p_sort_field FROM Params) LIKE 'HIT_COUNT%' AND (SELECT p_sort_field_value FROM Params) = 'desc' THEN hit_7 END DESC,
    CASE WHEN (SELECT p_sort_field FROM Params) LIKE 'HIT_COUNT%' AND (SELECT p_sort_field_value FROM Params) = 'asc'  THEN hit_7 END ASC,
    id ASC
		LIMIT 10;
