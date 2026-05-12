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
    10 AS p_agg_latest_limit,
    CAST([] AS ARRAY<STRING>) AS p_id_list,
    'th' AS p_language,
    CAST([] AS ARRAY<STRING>) AS p_exclude_ids,
    CAST([] AS ARRAY<STRING>) AS p_exclude_partner_related
  ),
-- ============================================================
-- BASE DATA + FILTERS
-- ============================================================
BaseDataRaw AS (
		SELECT
			m.id, m.publish_date, m.article_category, m.tags, m.create_by,
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
		  FROM mst_gamearticle_nonprod m
		WHERE
		    m.status = 'publish'
    AND m.searchable = 'Y'
    AND m.publish_date <= CURRENT_TIMESTAMP()
    AND m.title != 'empty title'
    AND (ARRAY_LENGTH((SELECT p_id_list FROM Params)) = 0 OR m.id IN UNNEST((SELECT p_id_list FROM Params)))
    AND m.lang = IFNULL((SELECT p_language FROM Params), 'th')
    AND (ARRAY_LENGTH((SELECT p_exclude_ids FROM Params)) = 0 OR m.id NOT IN UNNEST((SELECT p_exclude_ids FROM Params)))
    AND (ARRAY_LENGTH((SELECT p_exclude_partner_related FROM Params)) = 0
         OR NOT EXISTS(SELECT 1 FROM UNNEST(m.partner_related) AS x WHERE x IN UNNEST((SELECT p_exclude_partner_related FROM Params))))
		)
,
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
,

-- ============================================================
-- LATEST PER GROUP (ARRAY_AGG replaces ROW_NUMBER PARTITION BY)
-- ============================================================
LatestAggGrouped AS (
			SELECT
				agg_group_key,
				ARRAY_AGG(id ORDER BY publish_date DESC, id ASC LIMIT 10) AS ids -- Spanner ไม่รองรับ subquery ใน LIMIT ต้องแก้ตัวเลขนี้โดยตรง
			FROM BaseData
			GROUP BY agg_group_key
			),

			LatestAgg AS (
			SELECT
				b.id, b.publish_date, b.article_category, b.tags, b.create_by,
				lag.agg_group_key,
				'LATEST_AGG' AS result_type,
				CAST(1 AS INT64) AS agg_rank
			FROM LatestAggGrouped lag
			CROSS JOIN UNNEST(lag.ids) AS agg_id
			JOIN BaseData b ON b.id = agg_id
			)
-- ============================================================
-- FINAL SELECT
-- ============================================================
SELECT * FROM LatestAgg
ORDER BY id ASC;