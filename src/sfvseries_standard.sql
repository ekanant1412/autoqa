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
    365 AS p_tophit_date_filter,
    '' AS p_sort_field,
    '' AS p_sort_field_value,
    CAST(NULL AS FLOAT64) AS p_last_score,
    CAST(NULL AS TIMESTAMP) AS p_last_publish_date,
    '' AS p_last_id,
    10 AS p_limit
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
    FROM mst_sfvseries_nonprod, UNNEST(tags) AS tag
    WHERE ARRAY_LENGTH((SELECT p_more_likes FROM Params)) > 0
      AND 'tags' IN UNNEST((SELECT p_similar_fields FROM Params))
      AND mst_sfvseries_nonprod.id IN UNNEST((SELECT p_more_likes FROM Params))
    UNION ALL
    SELECT CAST(NULL AS STRING) AS t, CAST(cat AS STRING) AS c, CAST(NULL AS STRING) AS reference_title
    FROM mst_sfvseries_nonprod, UNNEST(article_category) AS cat
    WHERE ARRAY_LENGTH((SELECT p_more_likes FROM Params)) > 0
      AND 'article_category' IN UNNEST((SELECT p_similar_fields FROM Params))
      AND mst_sfvseries_nonprod.id IN UNNEST((SELECT p_more_likes FROM Params))
    UNION ALL
    SELECT CAST(NULL AS STRING) AS t, CAST(NULL AS STRING) AS c, CAST(title AS STRING) AS reference_title
    FROM mst_sfvseries_nonprod
    WHERE ARRAY_LENGTH((SELECT p_more_likes FROM Params)) > 0
      AND 'title' IN UNNEST((SELECT p_similar_fields FROM Params))
      AND mst_sfvseries_nonprod.id IN UNNEST((SELECT p_more_likes FROM Params))
  )
),
-- ============================================================
-- BASE DATA + FILTERS
-- ============================================================
BaseDataRaw AS (
		SELECT
			m.id, m.thumb, m.title, m.article_category, m.genres, m.sub_genres, m.content_type, m.create_date, m.publish_date, m.tags, m.source_country, m.lang, m.relate_content, m.create_by, m.update_by,
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
		  FROM mst_sfvseries_nonprod m
  CROSS JOIN ReferenceData ref
		WHERE
		    m.status = 'publish'
    AND m.searchable = 'Y'
    AND m.publish_date <= CURRENT_TIMESTAMP()
    AND (m.expire_date IS NULL OR m.expire_date > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE))
    AND m.title != 'empty title'
    AND m.lang = IFNULL((SELECT p_language FROM Params), 'th')
    AND ((SELECT p_multimatch_query FROM Params) IS NULL OR (SELECT p_multimatch_query FROM Params) = '')
    AND (ARRAY_LENGTH((SELECT p_exclude_ids FROM Params)) = 0 OR m.id NOT IN UNNEST((SELECT p_exclude_ids FROM Params)))
    AND (ARRAY_LENGTH((SELECT p_filter_out_category FROM Params)) = 0
         OR NOT EXISTS(SELECT 1 FROM UNNEST(m.article_category) AS cat WHERE cat IN UNNEST((SELECT p_filter_out_category FROM Params))))
    AND (ARRAY_LENGTH((SELECT p_exclude_partner_related FROM Params)) = 0
         OR NOT EXISTS(SELECT 1 FROM UNNEST(m.partner_related) AS pr WHERE pr IN UNNEST((SELECT p_exclude_partner_related FROM Params))))
    AND (ARRAY_LENGTH((SELECT p_genres FROM Params)) = 0 OR EXISTS(SELECT 1 FROM UNNEST(m.genres) AS g WHERE g IN UNNEST((SELECT p_genres FROM Params))))
    AND (ARRAY_LENGTH((SELECT p_article_category FROM Params)) = 0 OR EXISTS(SELECT 1 FROM UNNEST(m.article_category) AS cat WHERE cat IN UNNEST((SELECT p_article_category FROM Params))))
    AND (ARRAY_LENGTH((SELECT p_id_list FROM Params)) = 0 OR m.id IN UNNEST((SELECT p_id_list FROM Params)))
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
-- MAIN LIST
-- ============================================================
MainResults AS (
		SELECT
			id, thumb, title, article_category, genres, sub_genres, content_type, create_date, publish_date, tags, source_country, lang, relate_content, create_by, update_by,
			'MAIN_LIST' AS result_type,
			CAST(0 AS INT64) AS agg_rank
		FROM BaseData
		ORDER BY
		    CASE WHEN (SELECT p_is_random FROM Params) = TRUE THEN random_seed END DESC,
    CASE WHEN ARRAY_LENGTH((SELECT p_keymap_order FROM Params)) > 0 THEN keymap_priority END ASC,
    CASE WHEN (SELECT p_sort_field FROM Params) = 'publish_date' AND (SELECT p_sort_field_value FROM Params) = 'desc' THEN UNIX_MICROS(publish_date) END DESC,
    CASE WHEN (SELECT p_sort_field FROM Params) = 'publish_date' AND (SELECT p_sort_field_value FROM Params) = 'asc'  THEN UNIX_MICROS(publish_date) END ASC,
    CASE WHEN (SELECT p_sort_field FROM Params) LIKE 'HIT_COUNT%' AND (SELECT p_sort_field_value FROM Params) = 'desc' THEN hit_7 END DESC,
    CASE WHEN (SELECT p_sort_field FROM Params) LIKE 'HIT_COUNT%' AND (SELECT p_sort_field_value FROM Params) = 'asc'  THEN hit_7 END ASC,
    id ASC
		LIMIT 10
		)
-- ============================================================
-- FINAL SELECT
-- ============================================================
SELECT * FROM MainResults
ORDER BY id ASC;