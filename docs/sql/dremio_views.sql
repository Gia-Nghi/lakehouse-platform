CREATE OR REPLACE VIEW nessie.gold.vw_market_overview_daily AS
SELECT
    fp.snapshot_date,

    COUNT(*) AS row_count,
    COUNT(DISTINCT fp.list_id) AS listing_count,
    COUNT(DISTINCT fp.account_id) AS unique_seller_count,

    AVG(CAST(fp.price AS DOUBLE)) AS price_avg,
    AVG(CAST(fp.price_per_m2 AS DOUBLE)) AS price_per_m2_avg,
    AVG(CAST(fp.size AS DOUBLE)) AS size_avg,

    SUM(CASE WHEN fp.is_apt = 1 THEN 1 ELSE 0 END) AS apt_count,
    SUM(CASE WHEN fp.is_house = 1 THEN 1 ELSE 0 END) AS house_count,
    SUM(CASE WHEN fp.is_land = 1 THEN 1 ELSE 0 END) AS land_count,

    SUM(CASE 
        WHEN fp.is_apt = 0 
         AND fp.is_house = 0 
         AND fp.is_land = 0 
        THEN 1 ELSE 0 
    END) AS unknown_property_count

FROM nessie.gold.fact_properties fp
WHERE fp.snapshot_date IS NOT NULL
GROUP BY fp.snapshot_date;

CREATE OR REPLACE VIEW nessie.gold.vw_market_trend_analysis_base AS
SELECT
    fp.snapshot_date,

    fp.location_key,
    dl.area_name,
    dl.ward_name,
    dl.street_name,

    COUNT(*) AS row_count,
    COUNT(DISTINCT fp.list_id) AS listing_count,
    COUNT(DISTINCT fp.account_id) AS unique_seller_count,

    AVG(CAST(fp.price AS DOUBLE)) AS price_avg,
    AVG(CAST(fp.price_per_m2 AS DOUBLE)) AS price_per_m2_avg,
    AVG(CAST(fp.size AS DOUBLE)) AS size_avg,

    SUM(CASE WHEN fp.is_apt = 1 THEN 1 ELSE 0 END) AS apt_count,
    SUM(CASE WHEN fp.is_house = 1 THEN 1 ELSE 0 END) AS house_count,
    SUM(CASE WHEN fp.is_land = 1 THEN 1 ELSE 0 END) AS land_count,

    AVG(CAST(fp.quality_score AS DOUBLE)) AS quality_score_avg

FROM nessie.gold.fact_properties fp
LEFT JOIN nessie.gold.dim_location dl
    ON fp.location_key = dl.location_key
WHERE fp.snapshot_date IS NOT NULL
GROUP BY
    fp.snapshot_date,
    fp.location_key,
    dl.area_name,
    dl.ward_name,
    dl.street_name;


CREATE OR REPLACE VIEW nessie.gold.vw_metro_impact_analysis_detail AS
SELECT
    metro_proximity_category,
    metro_group,

    area_name,
    ward_name,
    station_id,
    station_name,

    nearest_metro_distance_m,

    row_count,
    listing_count,

    price_avg,
    price_per_m2_avg,
    size_avg,

    apt_count,
    house_count,
    land_count,
    other_property_count,

    CAST(apt_count AS DOUBLE) / CAST(listing_count AS DOUBLE) AS apt_ratio,
    CAST(house_count AS DOUBLE) / CAST(listing_count AS DOUBLE) AS house_ratio,
    CAST(land_count AS DOUBLE) / CAST(listing_count AS DOUBLE) AS land_ratio,
    CAST(other_property_count AS DOUBLE) / CAST(listing_count AS DOUBLE) AS other_property_ratio,

    CASE
        WHEN apt_count >= house_count
         AND apt_count >= land_count
         AND apt_count >= other_property_count
            THEN 'Can ho / Chung cu'

        WHEN house_count >= apt_count
         AND house_count >= land_count
         AND house_count >= other_property_count
            THEN 'Nha o'

        WHEN land_count >= apt_count
         AND land_count >= house_count
         AND land_count >= other_property_count
            THEN 'Dat'

        ELSE 'Khac'
    END AS dominant_property_type

FROM nessie.gold.vw_metro_impact_analysis
WHERE listing_count > 0;


CREATE OR REPLACE VIEW nessie.gold.vw_metro_impact_analysis AS
SELECT
    fma.metro_proximity_category,

    CASE
        WHEN fma.metro_proximity_category = 'Close_To_Metro'
            THEN 'Gan metro'
        WHEN fma.metro_proximity_category = 'No_Metro_In_Ward'
            THEN 'Khong gan metro'
        ELSE 'Khac'
    END AS metro_group,

    fma.area_name,
    fma.ward_name,

    fma.station_id,
    MAX(dms.station_name) AS station_name,

    MIN(CAST(fma.distance_to_metro_m AS DOUBLE)) AS nearest_metro_distance_m,

    COUNT(*) AS row_count,
    COUNT(DISTINCT fma.list_id) AS listing_count,

    AVG(CAST(fma.price AS DOUBLE)) AS price_avg,
    AVG(CAST(fma.price_per_m2 AS DOUBLE)) AS price_per_m2_avg,
    AVG(CAST(fma.size AS DOUBLE)) AS size_avg,

    SUM(CASE WHEN fma.category_name = 'Căn hộ/Chung cư' THEN 1 ELSE 0 END) AS apt_count,
    SUM(CASE WHEN fma.category_name = 'Nhà ở' THEN 1 ELSE 0 END) AS house_count,
    SUM(CASE WHEN fma.category_name = 'Đất' THEN 1 ELSE 0 END) AS land_count,

    SUM(CASE 
        WHEN fma.category_name NOT IN ('Căn hộ/Chung cư', 'Nhà ở', 'Đất')
          OR fma.category_name IS NULL
        THEN 1 ELSE 0 
    END) AS other_property_count

FROM nessie.gold.fact_metro_analysis fma

LEFT JOIN nessie.gold.dim_metro_station dms
    ON fma.station_id = dms.station_id

WHERE fma.metro_proximity_category IS NOT NULL

GROUP BY
    fma.metro_proximity_category,
    fma.area_name,
    fma.ward_name,
    fma.station_id;


CREATE OR REPLACE VIEW nessie.gold.vw_area_market_analysis_base AS
SELECT
    fp.location_key,

    dl.area_name,
    dl.ward_name,
    dl.street_name,
    dl.location_tier,

    COUNT(*) AS row_count,
    COUNT(DISTINCT fp.list_id) AS listing_count,
    COUNT(DISTINCT fp.account_id) AS unique_seller_count,

    AVG(CAST(fp.price AS DOUBLE)) AS price_avg,
    AVG(CAST(fp.price_per_m2 AS DOUBLE)) AS price_per_m2_avg,
    AVG(CAST(fp.size AS DOUBLE)) AS size_avg,

    AVG(CAST(fp.quality_score AS DOUBLE)) AS quality_score_avg,
    AVG(CAST(fp.market_saturation_score AS DOUBLE)) AS market_saturation_score_avg,
    AVG(CAST(fp.seller_credibility_score AS DOUBLE)) AS seller_credibility_score_avg,

    SUM(CASE WHEN fp.is_apt = 1 THEN 1 ELSE 0 END) AS apt_count,
    SUM(CASE WHEN fp.is_house = 1 THEN 1 ELSE 0 END) AS house_count,
    SUM(CASE WHEN fp.is_land = 1 THEN 1 ELSE 0 END) AS land_count,

    SUM(CASE
        WHEN fp.is_apt = 0
         AND fp.is_house = 0
         AND fp.is_land = 0
        THEN 1 ELSE 0
    END) AS unknown_property_count,

    SUM(CASE WHEN fp.good_quality = 1 THEN 1 ELSE 0 END) AS good_quality_count,
    SUM(CASE WHEN fp.risk_quality = 1 THEN 1 ELSE 0 END) AS risk_quality_count,
    SUM(CASE WHEN fp.low_quality = 1 THEN 1 ELSE 0 END) AS low_quality_count,

    SUM(CASE WHEN fp.fully_legal = 1 THEN 1 ELSE 0 END) AS fully_legal_count,
    SUM(CASE WHEN fp.risk_legal = 1 THEN 1 ELSE 0 END) AS risk_legal_count,

    SUM(CASE WHEN fp.is_company_ad = 1 THEN 1 ELSE 0 END) AS company_seller_count

FROM nessie.gold.fact_properties fp
LEFT JOIN nessie.gold.dim_location dl
    ON fp.location_key = dl.location_key
WHERE fp.location_key IS NOT NULL
GROUP BY
    fp.location_key,
    dl.area_name,
    dl.ward_name,
    dl.street_name,
    dl.location_tier;


CREATE OR REPLACE VIEW nessie.gold.vw_market_overview_daily_analysis AS
SELECT
    snapshot_date,

    row_count,
    listing_count,
    unique_seller_count,

    price_avg,
    price_per_m2_avg,
    size_avg,

    apt_count,
    house_count,
    land_count,
    unknown_property_count,

    CAST(apt_count AS DOUBLE) / CAST(listing_count AS DOUBLE) AS apt_ratio,
    CAST(house_count AS DOUBLE) / CAST(listing_count AS DOUBLE) AS house_ratio,
    CAST(land_count AS DOUBLE) / CAST(listing_count AS DOUBLE) AS land_ratio,
    CAST(unknown_property_count AS DOUBLE) / CAST(listing_count AS DOUBLE) AS unknown_property_ratio,

    CASE
        WHEN apt_count >= house_count
         AND apt_count >= land_count
         AND apt_count >= unknown_property_count
            THEN 'Can ho / Chung cu'

        WHEN house_count >= apt_count
         AND house_count >= land_count
         AND house_count >= unknown_property_count
            THEN 'Nha o'

        WHEN land_count >= apt_count
         AND land_count >= house_count
         AND land_count >= unknown_property_count
            THEN 'Dat'

        ELSE 'Khong xac dinh'
    END AS dominant_property_type,

    CASE
        WHEN apt_count >= house_count
         AND apt_count >= land_count
         AND apt_count >= unknown_property_count
            THEN CAST(apt_count AS DOUBLE) / CAST(listing_count AS DOUBLE)

        WHEN house_count >= apt_count
         AND house_count >= land_count
         AND house_count >= unknown_property_count
            THEN CAST(house_count AS DOUBLE) / CAST(listing_count AS DOUBLE)

        WHEN land_count >= apt_count
         AND land_count >= house_count
         AND land_count >= unknown_property_count
            THEN CAST(land_count AS DOUBLE) / CAST(listing_count AS DOUBLE)

        ELSE CAST(unknown_property_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
    END AS dominant_property_ratio

FROM nessie.gold.vw_market_overview_daily
WHERE listing_count > 0;



CREATE OR REPLACE VIEW nessie.gold.vw_market_trend_analysis AS
SELECT
    snapshot_date,

    location_key,
    area_name,
    ward_name,
    street_name,

    listing_count,
    unique_seller_count,
    price_avg,
    price_per_m2_avg,
    size_avg,

    apt_count,
    house_count,
    land_count,
    quality_score_avg,

    LAG(listing_count) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS prev_listing_count,

    listing_count - LAG(listing_count) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS listing_change,

    CASE
        WHEN LAG(listing_count) OVER (
            PARTITION BY location_key
            ORDER BY snapshot_date
        ) > 0
        THEN CAST(
            listing_count - LAG(listing_count) OVER (
                PARTITION BY location_key
                ORDER BY snapshot_date
            ) AS DOUBLE
        )
        /
        CAST(
            LAG(listing_count) OVER (
                PARTITION BY location_key
                ORDER BY snapshot_date
            ) AS DOUBLE
        )
        ELSE NULL
    END AS listing_growth_rate,

    LAG(price_per_m2_avg) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS prev_price_per_m2_avg,

    price_per_m2_avg - LAG(price_per_m2_avg) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS price_per_m2_change,

    CASE
        WHEN LAG(price_per_m2_avg) OVER (
            PARTITION BY location_key
            ORDER BY snapshot_date
        ) > 0
        THEN CAST(
            price_per_m2_avg - LAG(price_per_m2_avg) OVER (
                PARTITION BY location_key
                ORDER BY snapshot_date
            ) AS DOUBLE
        )
        /
        CAST(
            LAG(price_per_m2_avg) OVER (
                PARTITION BY location_key
                ORDER BY snapshot_date
            ) AS DOUBLE
        )
        ELSE NULL
    END AS price_per_m2_growth_rate,

    LAG(unique_seller_count) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS prev_unique_seller_count,

    unique_seller_count - LAG(unique_seller_count) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS seller_change,

    CASE
        WHEN LAG(unique_seller_count) OVER (
            PARTITION BY location_key
            ORDER BY snapshot_date
        ) > 0
        THEN CAST(
            unique_seller_count - LAG(unique_seller_count) OVER (
                PARTITION BY location_key
                ORDER BY snapshot_date
            ) AS DOUBLE
        )
        /
        CAST(
            LAG(unique_seller_count) OVER (
                PARTITION BY location_key
                ORDER BY snapshot_date
            ) AS DOUBLE
        )
        ELSE NULL
    END AS seller_growth_rate,

    LAG(apt_count) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS prev_apt_count,

    apt_count - LAG(apt_count) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS apt_change,

    LAG(house_count) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS prev_house_count,

    house_count - LAG(house_count) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS house_change,

    LAG(land_count) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS prev_land_count,

    land_count - LAG(land_count) OVER (
        PARTITION BY location_key
        ORDER BY snapshot_date
    ) AS land_change,

    CASE
        WHEN listing_count - LAG(listing_count) OVER (
            PARTITION BY location_key
            ORDER BY snapshot_date
        ) > 0
         AND price_per_m2_avg - LAG(price_per_m2_avg) OVER (
            PARTITION BY location_key
            ORDER BY snapshot_date
        ) > 0
            THEN 'Cung tang - Gia tang'

        WHEN listing_count - LAG(listing_count) OVER (
            PARTITION BY location_key
            ORDER BY snapshot_date
        ) > 0
         AND price_per_m2_avg - LAG(price_per_m2_avg) OVER (
            PARTITION BY location_key
            ORDER BY snapshot_date
        ) < 0
            THEN 'Cung tang - Gia giam'

        WHEN listing_count - LAG(listing_count) OVER (
            PARTITION BY location_key
            ORDER BY snapshot_date
        ) < 0
         AND price_per_m2_avg - LAG(price_per_m2_avg) OVER (
            PARTITION BY location_key
            ORDER BY snapshot_date
        ) > 0
            THEN 'Cung giam - Gia tang'

        WHEN listing_count - LAG(listing_count) OVER (
            PARTITION BY location_key
            ORDER BY snapshot_date
        ) < 0
         AND price_per_m2_avg - LAG(price_per_m2_avg) OVER (
            PARTITION BY location_key
            ORDER BY snapshot_date
        ) < 0
            THEN 'Cung giam - Gia giam'

        ELSE 'Khong ro xu huong'
    END AS supply_price_trend_type

FROM nessie.gold.vw_market_trend_analysis_base
WHERE listing_count > 0;


CREATE OR REPLACE VIEW nessie.gold.vw_market_trend_monthly_base AS

SELECT
    dt."year" AS year_value,
    dt."month" AS month_value,

    dt."year" * 100 + dt."month" AS month_key,

    fp.location_key,
    dl.area_name,
    dl.ward_name,
    dl.street_name,

    fp.property_key,
    dp.category_name AS property_type,

    COUNT(DISTINCT fp.list_id) AS listing_count,
    COUNT(DISTINCT fp.account_id) AS unique_seller_count,

    AVG(fp.price) AS price_avg,
    AVG(fp.price_per_m2) AS price_per_m2_avg,
    AVG(fp.size) AS size_avg,

    MIN(fp.price_per_m2) AS price_per_m2_min,
    MAX(fp.price_per_m2) AS price_per_m2_max,

    AVG(fp.quality_score) AS quality_score_avg

FROM nessie.gold.fact_properties fp

LEFT JOIN nessie.gold.dim_time dt
    ON fp.time_key = dt.time_key

LEFT JOIN nessie.gold.dim_location dl
    ON fp.location_key = dl.location_key

LEFT JOIN nessie.gold.dim_property dp
    ON fp.property_key = dp.property_key

WHERE fp.time_key IS NOT NULL
  AND fp.list_id IS NOT NULL
  AND fp.price_per_m2 IS NOT NULL
  AND fp.price_per_m2 > 0
  AND dt."year" IS NOT NULL
  AND dt."month" IS NOT NULL
  AND dl.area_name IS NOT NULL
  AND dp.category_name IS NOT NULL

GROUP BY
    dt."year",
    dt."month",
    dt."year" * 100 + dt."month",

    fp.location_key,
    dl.area_name,
    dl.ward_name,
    dl.street_name,

    fp.property_key,
    dp.category_name;


CREATE OR REPLACE VIEW nessie.gold.vw_market_trend_monthly_analysis AS

WITH current_month AS (
    SELECT
        year_value,
        month_value,
        month_key,

        CASE
            WHEN month_value = 1
                THEN (year_value - 1) * 100 + 12
            ELSE year_value * 100 + (month_value - 1)
        END AS prev_month_key,

        location_key,
        area_name,
        ward_name,
        street_name,

        property_key,
        property_type,

        listing_count,
        unique_seller_count,

        price_avg,
        price_per_m2_avg,
        size_avg,
        price_per_m2_min,
        price_per_m2_max,
        quality_score_avg

    FROM nessie.gold.vw_market_trend_monthly_base
),

previous_month AS (
    SELECT
        month_key,
        location_key,
        property_key,

        listing_count AS prev_listing_count,
        unique_seller_count AS prev_unique_seller_count,
        price_per_m2_avg AS prev_price_per_m2_avg

    FROM nessie.gold.vw_market_trend_monthly_base
)

SELECT
    c.year_value,
    c.month_value,
    c.month_key,

    c.location_key,
    c.area_name,
    c.ward_name,
    c.street_name,

    c.property_key,
    c.property_type,

    c.listing_count,
    p.prev_listing_count,

    c.listing_count - p.prev_listing_count AS listing_change,

    CASE
        WHEN p.prev_listing_count > 0
        THEN CAST(c.listing_count - p.prev_listing_count AS DOUBLE)
             / CAST(p.prev_listing_count AS DOUBLE)
        ELSE NULL
    END AS listing_growth_rate,

    c.unique_seller_count,
    p.prev_unique_seller_count,

    c.unique_seller_count - p.prev_unique_seller_count AS seller_change,

    CASE
        WHEN p.prev_unique_seller_count > 0
        THEN CAST(c.unique_seller_count - p.prev_unique_seller_count AS DOUBLE)
             / CAST(p.prev_unique_seller_count AS DOUBLE)
        ELSE NULL
    END AS seller_growth_rate,

    c.price_avg,
    c.price_per_m2_avg,
    p.prev_price_per_m2_avg,

    c.price_per_m2_avg - p.prev_price_per_m2_avg AS price_per_m2_change,

    CASE
        WHEN p.prev_price_per_m2_avg > 0
        THEN CAST(c.price_per_m2_avg - p.prev_price_per_m2_avg AS DOUBLE)
             / CAST(p.prev_price_per_m2_avg AS DOUBLE)
        ELSE NULL
    END AS price_per_m2_growth_rate,

    c.size_avg,
    c.price_per_m2_min,
    c.price_per_m2_max,
    c.quality_score_avg

FROM current_month c

LEFT JOIN previous_month p
    ON c.location_key = p.location_key
   AND c.property_key = p.property_key
   AND c.prev_month_key = p.month_key

WHERE c.listing_count > 0;


CREATE OR REPLACE VIEW nessie.gold.vw_hotspot_analysis AS

WITH current_month AS (
    SELECT
        year_value,
        month_value,
        month_order,
        month_key,

        location_key,
        area_name,
        ward_name,
        street_name,
        location_tier,

        property_type,

        listing_count,
        unique_seller_count,

        price_avg,
        price_per_m2_avg,
        size_avg,
        price_per_m2_min,
        price_per_m2_max,

        apt_count,
        house_count,
        land_count,

        quality_score_avg,
        seller_credibility_score_avg,
        market_saturation_score_avg,
        area_popularity_avg,

        number_of_images_avg,
        listing_age_days_avg,

        listings_with_images_count,
        listings_with_multiple_images_count,

        fully_legal_count,
        risk_legal_count,
        good_quality_count,
        risk_quality_count,
        company_seller_listing_count,

        CASE
            WHEN month_value = 1 THEN ((year_value - 1) * 100 + 12)
            ELSE (year_value * 100 + month_value - 1)
        END AS prev_month_order

    FROM nessie.gold.vw_hotspot_monthly_base
),

joined_month AS (
    SELECT
        cur.year_value,
        cur.month_value,
        cur.month_order,
        cur.month_key,

        cur.location_key,
        cur.area_name,
        cur.ward_name,
        cur.street_name,
        cur.location_tier,

        cur.property_type,

        cur.listing_count,
        pm.listing_count AS prev_listing_count,

        cur.unique_seller_count,
        pm.unique_seller_count AS prev_unique_seller_count,

        cur.price_avg,
        cur.price_per_m2_avg,
        pm.price_per_m2_avg AS prev_price_per_m2_avg,

        cur.size_avg,
        cur.price_per_m2_min,
        cur.price_per_m2_max,

        cur.apt_count,
        cur.house_count,
        cur.land_count,

        cur.quality_score_avg,
        cur.seller_credibility_score_avg,
        cur.market_saturation_score_avg,
        cur.area_popularity_avg,

        cur.number_of_images_avg,
        cur.listing_age_days_avg,

        cur.listings_with_images_count,
        cur.listings_with_multiple_images_count,

        cur.fully_legal_count,
        cur.risk_legal_count,
        cur.good_quality_count,
        cur.risk_quality_count,
        cur.company_seller_listing_count

    FROM current_month cur

    LEFT JOIN nessie.gold.vw_hotspot_monthly_base pm
        ON cur.location_key = pm.location_key
       AND cur.property_type = pm.property_type
       AND cur.prev_month_order = pm.month_order
),

growth_calc AS (
    SELECT
        year_value,
        month_value,
        month_order,
        month_key,

        location_key,
        area_name,
        ward_name,
        street_name,
        location_tier,

        property_type,

        listing_count,
        prev_listing_count,

        listing_count - prev_listing_count AS listing_change,

        CASE
            WHEN prev_listing_count IS NOT NULL AND prev_listing_count > 0
                THEN CAST(listing_count - prev_listing_count AS DOUBLE) / CAST(prev_listing_count AS DOUBLE)
            ELSE NULL
        END AS listing_growth_rate,

        unique_seller_count,
        prev_unique_seller_count,

        unique_seller_count - prev_unique_seller_count AS unique_seller_change,

        CASE
            WHEN prev_unique_seller_count IS NOT NULL AND prev_unique_seller_count > 0
                THEN CAST(unique_seller_count - prev_unique_seller_count AS DOUBLE) / CAST(prev_unique_seller_count AS DOUBLE)
            ELSE NULL
        END AS unique_seller_growth_rate,

        price_avg,
        price_per_m2_avg,
        prev_price_per_m2_avg,

        price_per_m2_avg - prev_price_per_m2_avg AS price_per_m2_change,

        CASE
            WHEN prev_price_per_m2_avg IS NOT NULL AND prev_price_per_m2_avg > 0
                THEN CAST(price_per_m2_avg - prev_price_per_m2_avg AS DOUBLE) / CAST(prev_price_per_m2_avg AS DOUBLE)
            ELSE NULL
        END AS price_per_m2_growth_rate,

        size_avg,
        price_per_m2_min,
        price_per_m2_max,

        apt_count,
        house_count,
        land_count,

        CASE
            WHEN listing_count > 0 THEN CAST(apt_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
            ELSE NULL
        END AS apt_ratio,

        CASE
            WHEN listing_count > 0 THEN CAST(house_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
            ELSE NULL
        END AS house_ratio,

        CASE
            WHEN listing_count > 0 THEN CAST(land_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
            ELSE NULL
        END AS land_ratio,

        CASE
            WHEN apt_count >= house_count AND apt_count >= land_count THEN 'Can ho / Chung cu'
            WHEN house_count >= apt_count AND house_count >= land_count THEN 'Nha o'
            WHEN land_count >= apt_count AND land_count >= house_count THEN 'Dat'
            ELSE 'Khong xac dinh'
        END AS dominant_property_type,

        CASE
            WHEN listing_count > 0 AND apt_count >= house_count AND apt_count >= land_count
                THEN CAST(apt_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
            WHEN listing_count > 0 AND house_count >= apt_count AND house_count >= land_count
                THEN CAST(house_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
            WHEN listing_count > 0 AND land_count >= apt_count AND land_count >= house_count
                THEN CAST(land_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
            ELSE NULL
        END AS dominant_property_ratio,

        quality_score_avg,
        seller_credibility_score_avg,
        market_saturation_score_avg,
        area_popularity_avg,

        number_of_images_avg,
        listing_age_days_avg,

        CASE
            WHEN listing_count > 0 THEN CAST(fully_legal_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
            ELSE NULL
        END AS fully_legal_ratio,

        CASE
            WHEN listing_count > 0 THEN CAST(risk_legal_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
            ELSE NULL
        END AS risk_legal_ratio,

        CASE
            WHEN listing_count > 0 THEN CAST(good_quality_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
            ELSE NULL
        END AS good_quality_ratio,

        CASE
            WHEN listing_count > 0 THEN CAST(risk_quality_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
            ELSE NULL
        END AS risk_quality_ratio,

        CASE
            WHEN listing_count > 0 THEN CAST(company_seller_listing_count AS DOUBLE) / CAST(listing_count AS DOUBLE)
            ELSE NULL
        END AS company_seller_ratio

    FROM joined_month
),

score_calc AS (
    SELECT
        *,

        CASE
            WHEN listing_count >= 30 THEN CAST(1.0 AS DOUBLE)
            WHEN listing_count >= 15 THEN CAST(0.8 AS DOUBLE)
            WHEN listing_count >= 8 THEN CAST(0.6 AS DOUBLE)
            WHEN listing_count >= 3 THEN CAST(0.4 AS DOUBLE)
            ELSE CAST(0.0 AS DOUBLE)
        END AS listing_volume_score,

        CASE
            WHEN listing_growth_rate >= 0.5 THEN CAST(1.0 AS DOUBLE)
            WHEN listing_growth_rate >= 0.3 THEN CAST(0.8 AS DOUBLE)
            WHEN listing_growth_rate >= 0.1 THEN CAST(0.6 AS DOUBLE)
            WHEN listing_growth_rate > 0 THEN CAST(0.4 AS DOUBLE)
            ELSE CAST(0.0 AS DOUBLE)
        END AS listing_growth_score,

        CASE
            WHEN price_per_m2_growth_rate >= 0.3 THEN CAST(1.0 AS DOUBLE)
            WHEN price_per_m2_growth_rate >= 0.15 THEN CAST(0.8 AS DOUBLE)
            WHEN price_per_m2_growth_rate >= 0.05 THEN CAST(0.6 AS DOUBLE)
            WHEN price_per_m2_growth_rate > 0 THEN CAST(0.4 AS DOUBLE)
            ELSE CAST(0.0 AS DOUBLE)
        END AS price_growth_score,

        CASE
            WHEN unique_seller_count >= 15 THEN CAST(1.0 AS DOUBLE)
            WHEN unique_seller_count >= 8 THEN CAST(0.8 AS DOUBLE)
            WHEN unique_seller_count >= 4 THEN CAST(0.6 AS DOUBLE)
            WHEN unique_seller_count >= 2 THEN CAST(0.4 AS DOUBLE)
            ELSE CAST(0.0 AS DOUBLE)
        END AS seller_score,

        CASE
            WHEN quality_score_avg >= 80 THEN CAST(1.0 AS DOUBLE)
            WHEN quality_score_avg >= 60 THEN CAST(0.8 AS DOUBLE)
            WHEN quality_score_avg >= 40 THEN CAST(0.6 AS DOUBLE)
            WHEN quality_score_avg > 0 THEN CAST(0.4 AS DOUBLE)
            ELSE CAST(0.0 AS DOUBLE)
        END AS quality_score_norm

    FROM growth_calc
),

final_calc AS (
    SELECT
        *,

        (
            listing_volume_score * 0.25 +
            listing_growth_score * 0.25 +
            price_growth_score * 0.20 +
            seller_score * 0.20 +
            quality_score_norm * 0.10
        ) AS hotspot_score

    FROM score_calc
)

SELECT
    year_value,
    month_value,
    month_order,
    month_key,

    location_key,
    area_name,
    ward_name,
    street_name,
    location_tier,

    property_type,

    listing_count,
    prev_listing_count,
    listing_change,
    listing_growth_rate,

    unique_seller_count,
    prev_unique_seller_count,
    unique_seller_change,
    unique_seller_growth_rate,

    price_avg,
    price_per_m2_avg,
    prev_price_per_m2_avg,
    price_per_m2_change,
    price_per_m2_growth_rate,
    price_per_m2_min,
    price_per_m2_max,

    size_avg,

    apt_count,
    house_count,
    land_count,
    apt_ratio,
    house_ratio,
    land_ratio,
    dominant_property_type,
    dominant_property_ratio,

    quality_score_avg,
    seller_credibility_score_avg,
    market_saturation_score_avg,
    area_popularity_avg,

    number_of_images_avg,
    listing_age_days_avg,

    fully_legal_ratio,
    risk_legal_ratio,
    good_quality_ratio,
    risk_quality_ratio,
    company_seller_ratio,

    listing_volume_score,
    listing_growth_score,
    price_growth_score,
    seller_score,
    quality_score_norm,

    hotspot_score,

    CASE
        WHEN hotspot_score >= 0.75
         AND listing_count >= 5
         AND listing_growth_rate > 0
         AND price_per_m2_growth_rate > 0
            THEN 'High'

        WHEN hotspot_score >= 0.50
         AND listing_count >= 3
            THEN 'Medium'

        ELSE 'Low'
    END AS hotspot_level,

    CASE
        WHEN listing_growth_rate > 0
         AND price_per_m2_growth_rate > 0
            THEN 1
        ELSE 0
    END AS is_supply_and_price_increase,

    CASE
        WHEN listing_count >= 5
         AND listing_growth_rate > 0
         AND price_per_m2_growth_rate > 0
         AND unique_seller_count >= 3
            THEN 1
        ELSE 0
    END AS is_potential_hotspot,

    CASE WHEN listing_count >= 5 THEN 1 ELSE 0 END AS has_high_listing_volume,
    CASE WHEN listing_growth_rate > 0 THEN 1 ELSE 0 END AS has_listing_growth,
    CASE WHEN price_per_m2_growth_rate > 0 THEN 1 ELSE 0 END AS has_price_growth,
    CASE WHEN unique_seller_growth_rate > 0 THEN 1 ELSE 0 END AS has_seller_growth

FROM final_calc
WHERE listing_count >= 3;


CREATE OR REPLACE VIEW nessie.gold.vw_hotspot_monthly_base AS

WITH property_base AS (
    SELECT
        fp.list_id,
        fp.account_id,
        fp.location_key,
        fp.time_key,

        fp.price,
        fp.price_per_m2,
        fp.size,

        fp.is_apt,
        fp.is_house,
        fp.is_land,

        fp.quality_score,
        fp.seller_credibility_score,
        fp.market_saturation_score,
        fp.area_popularity,

        fp.number_of_images,
        fp.listing_age_days,

        fp.has_images,
        fp.has_multiple_images,

        fp.fully_legal,
        fp.risk_legal,
        fp.good_quality,
        fp.risk_quality,
        fp.is_company_ad,

        dt."year" AS year_value,
        dt."month" AS month_value,
        (dt."year" * 100 + dt."month") AS month_order,

        CAST(dt."year" AS VARCHAR) || '-' ||
        CASE
            WHEN dt."month" < 10 THEN '0' || CAST(dt."month" AS VARCHAR)
            ELSE CAST(dt."month" AS VARCHAR)
        END AS month_key,

        dl.area_name,
        dl.ward_name,
        dl.street_name,
        dl.location_tier,

        CASE
            WHEN fp.is_apt = 1 THEN 'Can ho / Chung cu'
            WHEN fp.is_house = 1 THEN 'Nha o'
            WHEN fp.is_land = 1 THEN 'Dat'
            ELSE 'Khong xac dinh'
        END AS property_type

    FROM nessie.gold.fact_properties fp

    LEFT JOIN nessie.gold.dim_location dl
        ON fp.location_key = dl.location_key

    LEFT JOIN nessie.gold.dim_time dt
        ON fp.time_key = dt.time_key

    WHERE fp.location_key IS NOT NULL
      AND fp.time_key IS NOT NULL
      AND dl.area_name IS NOT NULL
      AND dt."year" IS NOT NULL
      AND dt."month" IS NOT NULL
      AND fp.price_per_m2 IS NOT NULL
)

SELECT
    year_value,
    month_value,
    month_order,
    month_key,

    location_key,
    area_name,
    ward_name,
    street_name,
    location_tier,

    property_type,

    COUNT(DISTINCT list_id) AS listing_count,
    COUNT(DISTINCT account_id) AS unique_seller_count,

    AVG(CAST(price AS DOUBLE)) AS price_avg,
    AVG(CAST(price_per_m2 AS DOUBLE)) AS price_per_m2_avg,
    AVG(CAST(size AS DOUBLE)) AS size_avg,

    MIN(CAST(price_per_m2 AS DOUBLE)) AS price_per_m2_min,
    MAX(CAST(price_per_m2 AS DOUBLE)) AS price_per_m2_max,

    SUM(CASE WHEN is_apt = 1 THEN 1 ELSE 0 END) AS apt_count,
    SUM(CASE WHEN is_house = 1 THEN 1 ELSE 0 END) AS house_count,
    SUM(CASE WHEN is_land = 1 THEN 1 ELSE 0 END) AS land_count,

    AVG(CAST(quality_score AS DOUBLE)) AS quality_score_avg,
    AVG(CAST(seller_credibility_score AS DOUBLE)) AS seller_credibility_score_avg,
    AVG(CAST(market_saturation_score AS DOUBLE)) AS market_saturation_score_avg,
    AVG(CAST(area_popularity AS DOUBLE)) AS area_popularity_avg,

    AVG(CAST(number_of_images AS DOUBLE)) AS number_of_images_avg,
    AVG(CAST(listing_age_days AS DOUBLE)) AS listing_age_days_avg,

    SUM(CASE WHEN has_images = 1 THEN 1 ELSE 0 END) AS listings_with_images_count,
    SUM(CASE WHEN has_multiple_images = 1 THEN 1 ELSE 0 END) AS listings_with_multiple_images_count,

    SUM(CASE WHEN fully_legal = 1 THEN 1 ELSE 0 END) AS fully_legal_count,
    SUM(CASE WHEN risk_legal = 1 THEN 1 ELSE 0 END) AS risk_legal_count,
    SUM(CASE WHEN good_quality = 1 THEN 1 ELSE 0 END) AS good_quality_count,
    SUM(CASE WHEN risk_quality = 1 THEN 1 ELSE 0 END) AS risk_quality_count,
    SUM(CASE WHEN is_company_ad = 1 THEN 1 ELSE 0 END) AS company_seller_listing_count

FROM property_base

GROUP BY
    year_value,
    month_value,
    month_order,
    month_key,
    location_key,
    area_name,
    ward_name,
    street_name,
    location_tier,
    property_type;