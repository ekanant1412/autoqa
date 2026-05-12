@{OPTIMIZER_VERSION=8}
WITH
  Params AS (
  SELECT
    CAST([] AS ARRAY<STRING>) AS p_more_likes,
    CAST([] AS ARRAY<STRING>) AS p_similar_fields,
    'th' AS p_language,
    FALSE AS p_is_random,
    CAST([] AS ARRAY<STRUCT<id STRING, priority INT64>>) AS p_keymap_order,
    '' AS p_multimatch_query,
    CAST([] AS ARRAY<STRING>) AS p_id_list,
    CAST([] AS ARRAY<STRING>) AS p_exclude_ids,
    CAST([] AS ARRAY<STRING>) AS p_filter_out_category,
    CAST([] AS ARRAY<STRING>) AS p_exclude_partner_related,
    CAST([] AS ARRAY<STRING>) AS p_genres,
    CAST([] AS ARRAY<STRING>) AS p_article_category,
    FALSE AS p_is_related_ecommerce,
    CAST([] AS ARRAY<STRING>) AS p_category_date_filter_target,
    7 AS p_category_date_filter_day,
    365 AS p_tophit_date_filter,
    '' AS p_sort_field,
    '' AS p_sort_field_value,
    CAST(NULL AS FLOAT64) AS p_last_score,
    CAST(NULL AS TIMESTAMP) AS p_last_publish_date,
    '' AS p_last_id,
    50 AS p_agg_latest_limit
  ),
-- ============================================================
-- REFERENCE DATA (More Like This)
-- ============================================================
ReferenceData AS (
  SELECT
    IF(ARRAY_LENGTH((SELECT p_more_likes FROM Params)) > 0,
      ARRAY_AGG(DISTINCT t), CAST([] AS ARRAY<STRING>)) AS reference_tags,
    IF(ARRAY_LENGTH((SELECT p_more_likes FROM Params)) > 0,
      ARRAY_AGG(DISTINCT c), CAST([] AS ARRAY<STRING>)) AS reference_categories,
    IF(ARRAY_LENGTH((SELECT p_more_likes FROM Params)) > 0,
      MAX(reference_title), NULL) AS reference_title
  FROM (
    SELECT CAST(tag AS STRING) AS t, CAST(NULL AS STRING) AS c, CAST(NULL AS STRING) AS reference_title
    FROM mst_ugcsfv_nonprod, UNNEST(tags) AS tag
    WHERE ARRAY_LENGTH((SELECT p_more_likes FROM Params)) > 0
      AND 'tags' IN UNNEST((SELECT p_similar_fields FROM Params))
      AND mst_ugcsfv_nonprod.id IN UNNEST((SELECT p_more_likes FROM Params))
    UNION ALL
    SELECT CAST(NULL AS STRING) AS t, CAST(cat AS STRING) AS c, CAST(NULL AS STRING) AS reference_title
    FROM mst_ugcsfv_nonprod, UNNEST(article_category) AS cat
    WHERE ARRAY_LENGTH((SELECT p_more_likes FROM Params)) > 0
      AND 'article_category' IN UNNEST((SELECT p_similar_fields FROM Params))
      AND mst_ugcsfv_nonprod.id IN UNNEST((SELECT p_more_likes FROM Params))
    UNION ALL
    SELECT CAST(NULL AS STRING) AS t, CAST(NULL AS STRING) AS c, CAST(title AS STRING) AS reference_title
    FROM mst_ugcsfv_nonprod
    WHERE ARRAY_LENGTH((SELECT p_more_likes FROM Params)) > 0
      AND 'title' IN UNNEST((SELECT p_similar_fields FROM Params))
      AND mst_ugcsfv_nonprod.id IN UNNEST((SELECT p_more_likes FROM Params))
  )
),
-- ============================================================
-- BASE DATA + FILTERS
-- ============================================================
BaseDataRaw AS (
		SELECT
			m.id, m.title, m.publish_date, m.article_category, m.tags, m.create_by,
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
			CASE
      WHEN ARRAY_LENGTH((SELECT p_more_likes FROM Params)) > 0 THEN
        CAST(
          CASE WHEN 'tags' IN UNNEST((SELECT p_similar_fields FROM Params)) AND ARRAY_LENGTH(ref.reference_tags) > 0
               THEN (SELECT COUNT(*) FROM UNNEST(m.tags) AS t WHERE t IN UNNEST(ref.reference_tags))
               ELSE 0 END
          + CASE WHEN 'article_category' IN UNNEST((SELECT p_similar_fields FROM Params)) AND ARRAY_LENGTH(ref.reference_categories) > 0
                 THEN (SELECT COUNT(*) FROM UNNEST(m.article_category) AS c WHERE c IN UNNEST(ref.reference_categories))
                 ELSE 0 END
        AS FLOAT64)
      ELSE 0.0
    END AS combined_score,
			COALESCE(
      (SELECT priority FROM UNNEST((SELECT p_keymap_order FROM Params)) WHERE id = m.id),
      999999
    ) AS keymap_priority,
			ABS(FARM_FINGERPRINT(m.id)) AS random_seed
		  FROM mst_ugcsfv_nonprod m
  CROSS JOIN ReferenceData ref
		WHERE
		    m.status = 'publish'
    AND m.searchable = 'Y'
    AND m.publish_date <= CURRENT_TIMESTAMP()
    AND m.title != 'empty title'
    AND m.lang = IFNULL((SELECT p_language FROM Params), 'th')
    AND ((SELECT p_multimatch_query FROM Params) IS NULL OR (SELECT p_multimatch_query FROM Params) = '')
    AND (ARRAY_LENGTH((SELECT p_exclude_ids FROM Params)) = 0 OR m.id NOT IN UNNEST((SELECT p_exclude_ids FROM Params)))
    AND (ARRAY_LENGTH((SELECT p_filter_out_category FROM Params)) = 0
         OR NOT EXISTS(SELECT 1 FROM UNNEST(m.article_category) AS cat WHERE cat IN UNNEST((SELECT p_filter_out_category FROM Params))))
    AND (ARRAY_LENGTH((SELECT p_exclude_partner_related FROM Params)) = 0
         OR NOT EXISTS(SELECT 1 FROM UNNEST(m.partner_related) AS pr WHERE pr IN UNNEST((SELECT p_exclude_partner_related FROM Params))))
    AND (ARRAY_LENGTH((SELECT p_article_category FROM Params)) = 0 OR EXISTS(SELECT 1 FROM UNNEST(m.article_category) AS cat WHERE cat IN UNNEST((SELECT p_article_category FROM Params))))
    AND (ARRAY_LENGTH((SELECT p_id_list FROM Params)) = 0 OR m.id IN UNNEST((SELECT p_id_list FROM Params)))
    AND ((SELECT p_is_related_ecommerce FROM Params) = FALSE OR ARRAY_LENGTH(m.related_ecommerce_id) > 0)
    AND (
      (EXISTS(SELECT 1 FROM UNNEST(m.article_category) AS cat WHERE cat IN UNNEST((SELECT p_category_date_filter_target FROM Params)))
       AND m.publish_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL (SELECT p_category_date_filter_day FROM Params) DAY))
      OR NOT EXISTS(SELECT 1 FROM UNNEST(m.article_category) AS cat WHERE cat IN UNNEST((SELECT p_category_date_filter_target FROM Params)))
    )
    AND ((SELECT p_tophit_date_filter FROM Params) IS NULL OR (SELECT p_tophit_date_filter FROM Params) = 0 OR m.publish_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL (SELECT p_tophit_date_filter FROM Params) DAY))
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
			),
			-- ============================================================
-- LATEST PER GROUP (ARRAY_AGG replaces ROW_NUMBER PARTITION BY)
-- ============================================================
LatestAggGrouped AS (
			SELECT
				agg_group_key,
				ARRAY_AGG(id ORDER BY publish_date DESC, id ASC LIMIT 50) AS ids -- Spanner ไม่รองรับ subquery ใน LIMIT ต้องแก้ตัวเลขนี้โดยตรง
			FROM BaseData
			GROUP BY agg_group_key
			),

			LatestAgg AS (
			SELECT
				b.id, b.title, b.publish_date, b.article_category, b.tags, b.create_by,
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