@{OPTIMIZER_VERSION=8}
WITH
  Params AS (
  SELECT
    10 AS p_limit,  -- ⚠️  To change: Edit LIMIT at bottom of SQL directly,
    CAST([] AS ARRAY<STRING>) AS p_id_list
  ),
-- ============================================================
-- BASE DATA + FILTERS
-- ============================================================
BaseDataRaw AS (
		SELECT
			m.*,
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
			
			
			ABS(FARM_FINGERPRINT(m.id)) AS random_seed
		  FROM mst_shelf_progressive_nonprod m
		WHERE
		    m.status = 'publish'
    AND m.publish_date <= CURRENT_TIMESTAMP()
    AND (m.expire_date IS NULL OR m.expire_date > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE))
    AND m.title != 'empty title'
    AND (ARRAY_LENGTH((SELECT p_id_list FROM Params)) = 0 OR m.id IN UNNEST((SELECT p_id_list FROM Params)))
		
),
-- ============================================================
-- BASE DATA
-- ============================================================
BaseData AS (
  SELECT * FROM BaseDataRaw
)

-- ============================================================
-- FINAL SELECT
-- ============================================================
SELECT *,
			'MAIN_LIST' AS result_type,
			CAST(0 AS INT64) AS agg_rank
		FROM BaseData
		ORDER BY
			    id ASC
		LIMIT 10;
