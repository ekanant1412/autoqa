@{OPTIMIZER_VERSION=8}
WITH
  Params AS (
  SELECT
    FALSE AS p_is_random,
    CAST([] AS ARRAY<STRUCT<id STRING, priority INT64>>) AS p_keymap_order,
    CAST([] AS ARRAY<STRUCT<id STRING, score FLOAT64>>) AS p_predictions,
    '' AS p_sort_field,
    '' AS p_sort_field_value,
    CAST(NULL AS FLOAT64) AS p_last_score,
    CAST(NULL AS TIMESTAMP) AS p_last_publish_date,
    '' AS p_last_id,
    10 AS p_limit,  -- ⚠️  To change: Edit LIMIT at bottom of SQL directly,
    '' AS p_ep_master,
    CAST([] AS ARRAY<STRING>) AS p_id_list,
    'th' AS p_language,
    CAST([] AS ARRAY<STRING>) AS p_genres,
    CAST([] AS ARRAY<STRING>) AS p_other_type,
    '' AS p_release_year_gte,
    '' AS p_release_year_lt,
    CAST([] AS ARRAY<STRING>) AS p_content_rights,
    CAST([] AS ARRAY<STRING>) AS p_article_category,
    CAST([] AS ARRAY<STRING>) AS p_rate,
    CAST([] AS ARRAY<STRING>) AS p_package_alacarte_list,
    '' AS p_title,
    CAST([] AS ARRAY<STRING>) AS p_exclude_ids,
    '' AS p_is_vod_layer,
    '' AS p_movie_type
  ),
-- ============================================================
-- BASE DATA + FILTERS
-- ============================================================
BaseDataRaw AS (
		SELECT
			m.id, m.release_year, m.thumb, m.tvod_flag, m.subscription_tiers, m.ep_items, m.movie_type, m.thumb_list, m.rate, m.audio, m.subtitle, m.duration, m.drm, m.display_qualities, m.trailer, m.content_rights, m.other_type, m.newepi_badge_start, m.newepi_badge_end, m.newepi_badge_type, m.title, m.actor, m.detail, m.synopsis, m.article_category, m.genres, m.package_alacarte, m.sub_genres, m.content_type, m.count_likes, m.count_views, m.partner_related, m.exclusive_badge_type, m.exclusive_badge_start, m.exclusive_badge_end, m.expire_date, m.publish_date, m.most_popular,
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
			COALESCE(
      (SELECT score FROM UNNEST((SELECT p_predictions FROM Params)) WHERE id = m.id),
      0.0
    ) AS predictions_score,
			ABS(FARM_FINGERPRINT(m.id)) AS random_seed
		  FROM mst_movie_nonprod m
		WHERE
		    m.status = 'publish'
    AND m.publish_date <= CURRENT_TIMESTAMP()
    AND m.searchable = 'Y'
    AND (m.expire_date IS NULL OR m.expire_date > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE))
    AND m.title != 'empty title'
    AND ((SELECT p_ep_master FROM Params) IS NULL OR (SELECT p_ep_master FROM Params) = '' OR m.ep_master = (SELECT p_ep_master FROM Params))
    AND (ARRAY_LENGTH((SELECT p_id_list FROM Params)) = 0 OR m.id IN UNNEST((SELECT p_id_list FROM Params)))
    AND m.lang = IFNULL((SELECT p_language FROM Params), 'th')
    AND (ARRAY_LENGTH((SELECT p_genres FROM Params)) = 0 OR EXISTS(SELECT 1 FROM UNNEST(m.genres) AS x WHERE x IN UNNEST((SELECT p_genres FROM Params))))
    AND (ARRAY_LENGTH((SELECT p_other_type FROM Params)) = 0 OR m.other_type IN UNNEST((SELECT p_other_type FROM Params)))
    AND ((SELECT p_release_year_gte FROM Params) IS NULL OR (SELECT p_release_year_gte FROM Params) = '' OR m.release_year = (SELECT p_release_year_gte FROM Params))
    AND ((SELECT p_release_year_lt FROM Params) IS NULL OR (SELECT p_release_year_lt FROM Params) = '' OR m.release_year = (SELECT p_release_year_lt FROM Params))
    AND (ARRAY_LENGTH((SELECT p_content_rights FROM Params)) = 0 OR m.content_rights IN UNNEST((SELECT p_content_rights FROM Params)))
    AND (ARRAY_LENGTH((SELECT p_article_category FROM Params)) = 0 OR EXISTS(SELECT 1 FROM UNNEST(m.article_category) AS x WHERE x IN UNNEST((SELECT p_article_category FROM Params))))
    AND (ARRAY_LENGTH((SELECT p_rate FROM Params)) = 0 OR m.rate IN UNNEST((SELECT p_rate FROM Params)))
    AND (ARRAY_LENGTH((SELECT p_package_alacarte_list FROM Params)) = 0 OR EXISTS(SELECT 1 FROM UNNEST(m.package_alacarte_list) AS x WHERE x IN UNNEST((SELECT p_package_alacarte_list FROM Params))))
    AND ((SELECT p_title FROM Params) IS NULL OR (SELECT p_title FROM Params) = '' OR m.title = (SELECT p_title FROM Params))
    AND (ARRAY_LENGTH((SELECT p_exclude_ids FROM Params)) = 0 OR m.id NOT IN UNNEST((SELECT p_exclude_ids FROM Params)))
    AND ((SELECT p_is_vod_layer FROM Params) IS NULL OR (SELECT p_is_vod_layer FROM Params) = '' OR m.is_vod_layer = (SELECT p_is_vod_layer FROM Params))
    AND ((SELECT p_movie_type FROM Params) IS NULL OR (SELECT p_movie_type FROM Params) = '' OR m.movie_type = (SELECT p_movie_type FROM Params))
    AND (m.publish_date <= CURRENT_TIMESTAMP())
    AND m.content_provider NOT IN ('true_vision')
    AND (m.expire_date > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE))
		
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
			id, release_year, thumb, tvod_flag, subscription_tiers, ep_items, movie_type, thumb_list, rate, audio, subtitle, duration, drm, display_qualities, trailer, content_rights, other_type, newepi_badge_start, newepi_badge_end, newepi_badge_type, title, actor, detail, synopsis, article_category, genres, package_alacarte, sub_genres, content_type, count_likes, count_views, partner_related, exclusive_badge_type, exclusive_badge_start, exclusive_badge_end, expire_date, publish_date, most_popular,
			'MAIN_LIST' AS result_type,
			CAST(0 AS INT64) AS agg_rank
		FROM BaseData
		ORDER BY
		    CASE WHEN (SELECT p_is_random FROM Params) = TRUE THEN random_seed END DESC,
    CASE WHEN ARRAY_LENGTH((SELECT p_keymap_order FROM Params)) > 0 THEN keymap_priority END ASC,
    CASE WHEN ARRAY_LENGTH((SELECT p_predictions FROM Params)) > 0 THEN predictions_score END DESC,
    CASE WHEN (SELECT p_sort_field FROM Params) = 'publish_date' AND (SELECT p_sort_field_value FROM Params) = 'desc' THEN UNIX_MICROS(publish_date) END DESC,
    CASE WHEN (SELECT p_sort_field FROM Params) = 'publish_date' AND (SELECT p_sort_field_value FROM Params) = 'asc'  THEN UNIX_MICROS(publish_date) END ASC,
    CASE WHEN (SELECT p_sort_field FROM Params) LIKE 'HIT_COUNT%' AND (SELECT p_sort_field_value FROM Params) = 'desc' THEN hit_7 END DESC,
    CASE WHEN (SELECT p_sort_field FROM Params) LIKE 'HIT_COUNT%' AND (SELECT p_sort_field_value FROM Params) = 'asc'  THEN hit_7 END ASC,
    id ASC
		LIMIT 10;
