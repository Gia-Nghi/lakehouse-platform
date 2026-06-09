
-----------------------1. Thị trường bất động sản đang diễn biến như thế nào theo ngày? --------
-- vw_market_overview_daily
-- vw_market_overview_daily_analysis

--- Xem ngày thị trường sôi động nhất --
SELECT
    snapshot_date,
    listing_count,
    unique_seller_count,
    price_avg,
    price_per_m2_avg,
    size_avg,
    dominant_property_type,
    dominant_property_ratio
FROM nessie.gold.vw_market_overview_daily_analysis
ORDER BY listing_count DESC
LIMIT 10;

--- Xem dữ liệu theo ngày ---
SELECT *
FROM nessie.gold.vw_market_overview_daily_analysis
ORDER BY snapshot_date;

--- Xem biến động số tin và người bán theo ngày ---
SELECT
    snapshot_date,
    listing_count,
    unique_seller_count,
    price_per_m2_avg,
    dominant_property_type
FROM nessie.gold.vw_market_overview_daily_analysis
ORDER BY snapshot_date;

--- Mỗi ngày có bao nhiêu tin đăng? ---
SELECT
    snapshot_date,
    listing_count
FROM nessie.gold.vw_market_overview_daily_analysis
ORDER BY snapshot_date;

		-- Ngày có tin đăng nhiều nhất --
SELECT
    snapshot_date,
    listing_count
FROM nessie.gold.vw_market_overview_daily_analysis
ORDER BY listing_count DESC
LIMIT 10;

--- Giá trung bình và giá/m² trung bình là bao nhiêu? Xem ngày có giá/m² trung bình cao nhất ---
SELECT
    snapshot_date,
    price_avg,
    price_per_m2_avg
FROM nessie.gold.vw_market_overview_daily_analysis
ORDER BY price_per_m2_avg DESC
LIMIT 10;

--- Loại hình chiếm ưu thế theo từng ngày? ---
SELECT
    snapshot_date,
    dominant_property_type,
    dominant_property_ratio
FROM nessie.gold.vw_market_overview_daily_analysis
ORDER BY snapshot_date;

--- Các ngày căn hộ/chung cư chiếm tỷ trọng cao nhất ---
SELECT
    snapshot_date,
    apt_count,
    apt_ratio,
    listing_count
FROM nessie.gold.vw_market_overview_daily_analysis
ORDER BY apt_ratio DESC
LIMIT 10;

--- Các ngày nhà ở chiếm tỷ trọng cao nhất ---
SELECT
    snapshot_date,
    house_count,
    house_ratio,
    listing_count
FROM nessie.gold.vw_market_overview_daily_analysis
ORDER BY house_ratio DESC
LIMIT 10;

--- Các ngày đất chiếm tỷ trọng cao nhất---
SELECT
    snapshot_date,
    land_count,
    land_ratio,
    listing_count
FROM nessie.gold.vw_market_overview_daily_analysis
ORDER BY land_ratio DESC
LIMIT 10;

--- So sánh số người bán với ngày trước
SELECT
    snapshot_date,
    unique_seller_count,

    LAG(unique_seller_count) OVER (
        ORDER BY snapshot_date
    ) AS prev_day_unique_seller_count,

    unique_seller_count 
    - LAG(unique_seller_count) OVER (
        ORDER BY snapshot_date
    ) AS seller_change,

    CAST(
        unique_seller_count 
        - LAG(unique_seller_count) OVER (
            ORDER BY snapshot_date
        ) AS DOUBLE
    )
    /
    CAST(
        LAG(unique_seller_count) OVER (
            ORDER BY snapshot_date
        ) AS DOUBLE
    ) AS seller_growth_rate

FROM nessie.gold.vw_market_overview_daily_analysis
ORDER BY snapshot_date;

-----------------------2.  Khu vực nào có hoạt động bất động sản nổi bật? --------------
-- vw_area_market_analysis_base
-- vw_area_market_analysis

-- 1. Quận/phường/đường nào có nhiều tin đăng?
SELECT
    area_name,
    ward_name,
    street_name,
    listing_count,
    unique_seller_count,
    price_per_m2_avg,
    dominant_property_type
FROM nessie.gold.vw_area_market_analysis
ORDER BY listing_count DESC
LIMIT 20;

-- 2. Khu vực nào có giá/m² cao?
SELECT
    area_name,
    ward_name,
    street_name,
    listing_count,
    price_avg,
    price_per_m2_avg,
    size_avg,
    dominant_property_type
FROM nessie.gold.vw_area_market_analysis
WHERE listing_count >= 5
ORDER BY price_per_m2_avg DESC
LIMIT 20;

-- Mình thêm WHERE listing_count >= 5 để tránh trường hợp khu vực chỉ có 1 tin nhưng giá quá cao làm sai lệch phân tích.

-- 3. Khu vực nào có nhiều người bán tham gia?
SELECT
    area_name,
    ward_name,
    street_name,
    listing_count,
    unique_seller_count,
    price_per_m2_avg,
    dominant_property_type
FROM nessie.gold.vw_area_market_analysis
ORDER BY unique_seller_count DESC
LIMIT 20;

-- 4. Khu vực nào tập trung nhiều căn hộ, nhà ở hoặc đất?
Khu vực tập trung nhiều căn hộ/chung cư
SELECT
    area_name,
    ward_name,
    street_name,
    listing_count,
    apt_count,
    apt_ratio,
    price_per_m2_avg
FROM nessie.gold.vw_area_market_analysis
WHERE listing_count >= 5
ORDER BY apt_count DESC
LIMIT 20;

-- Khu vực tập trung nhiều nhà ở
SELECT
    area_name,
    ward_name,
    street_name,
    listing_count,
    house_count,
    house_ratio,
    price_per_m2_avg
FROM nessie.gold.vw_area_market_analysis
WHERE listing_count >= 5
ORDER BY house_count DESC
LIMIT 20;

-- Khu vực tập trung nhiều đất
SELECT
    area_name,
    ward_name,
    street_name,
    listing_count,
    land_count,
    land_ratio,
    price_per_m2_avg
FROM nessie.gold.vw_area_market_analysis
WHERE listing_count >= 5
ORDER BY land_count DESC
LIMIT 20;

-- Xem loại hình chiếm ưu thế theo từng khu vực
SELECT
    area_name,
    ward_name,
    street_name,
    listing_count,
    dominant_property_type,
    dominant_property_ratio,
    apt_ratio,
    house_ratio,
    land_ratio
FROM nessie.gold.vw_area_market_analysis
WHERE listing_count >= 5
ORDER BY listing_count DESC
LIMIT 30;

-- 5. Khu vực nào có chất lượng tin đăng tốt hơn?
SELECT
    area_name,
    ward_name,
    street_name,
    listing_count,
    quality_score_avg,
    good_quality_ratio,
    risk_quality_ratio,
    fully_legal_ratio,
    risk_legal_ratio,
    price_per_m2_avg
FROM nessie.gold.vw_area_market_analysis
WHERE listing_count >= 5
ORDER BY quality_score_avg DESC
LIMIT 20;

-- Nếu muốn ưu tiên khu vực vừa nhiều tin vừa chất lượng tốt:

SELECT
    area_name,
    ward_name,
    street_name,
    listing_count,
    unique_seller_count,
    quality_score_avg,
    good_quality_ratio,
    fully_legal_ratio,
    price_per_m2_avg,
    dominant_property_type
FROM nessie.gold.vw_area_market_analysis
WHERE listing_count >= 5
ORDER BY quality_score_avg DESC, listing_count DESC
LIMIT 20;


-----------------------3. Thị trường đang tăng hay giảm theo thời gian? --------------
-- vw_market_trend_analysis_base
-- vw_market_trend_analysis

-- 1. Số lượng tin đăng tăng hay giảm so với ngày trước?
SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,
    listing_count,
    prev_listing_count,
    listing_change,
    listing_growth_rate
FROM nessie.gold.vw_market_trend_analysis
WHERE prev_listing_count IS NOT NULL
ORDER BY snapshot_date, listing_growth_rate DESC;

-- Xem khu vực tăng số tin mạnh nhất:

SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,
    listing_count,
    prev_listing_count,
    listing_change,
    listing_growth_rate
FROM nessie.gold.vw_market_trend_analysis
WHERE prev_listing_count IS NOT NULL
  AND prev_listing_count >= 3
ORDER BY listing_growth_rate DESC
LIMIT 20;
-- 2. Giá/m² tăng hay giảm?
SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,
    price_per_m2_avg,
    prev_price_per_m2_avg,
    price_per_m2_change,
    price_per_m2_growth_rate
FROM nessie.gold.vw_market_trend_analysis
WHERE prev_price_per_m2_avg IS NOT NULL
ORDER BY snapshot_date, price_per_m2_growth_rate DESC;

-- Xem khu vực tăng giá/m² mạnh nhất:

SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,
    listing_count,
    price_per_m2_avg,
    prev_price_per_m2_avg,
    price_per_m2_change,
    price_per_m2_growth_rate
FROM nessie.gold.vw_market_trend_analysis
WHERE prev_price_per_m2_avg IS NOT NULL
  AND prev_listing_count >= 3
  AND listing_count >= 3
ORDER BY price_per_m2_growth_rate DESC
LIMIT 20;

-- 3. Khu vực nào có tốc độ tăng trưởng nhanh?

-- Ở đây nên xét kết hợp cả tăng số tin và tăng giá/m².

SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,

    listing_count,
    prev_listing_count,
    listing_growth_rate,

    price_per_m2_avg,
    prev_price_per_m2_avg,
    price_per_m2_growth_rate,

    unique_seller_count,
    seller_growth_rate,

    supply_price_trend_type
FROM nessie.gold.vw_market_trend_analysis
WHERE prev_listing_count >= 3
  AND listing_count >= 3
  AND listing_growth_rate > 0
  AND price_per_m2_growth_rate > 0
ORDER BY listing_growth_rate DESC, price_per_m2_growth_rate DESC
LIMIT 20;

-- 4. Loại hình nào đang có xu hướng tăng mạnh?

-- Xem loại hình tăng mạnh theo từng khu vực:

SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,

    apt_count,
    prev_apt_count,
    apt_change,

    house_count,
    prev_house_count,
    house_change,

    land_count,
    prev_land_count,
    land_change,

    CASE
        WHEN apt_change >= house_change AND apt_change >= land_change THEN 'Can ho / Chung cu'
        WHEN house_change >= apt_change AND house_change >= land_change THEN 'Nha o'
        WHEN land_change >= apt_change AND land_change >= house_change THEN 'Dat'
        ELSE 'Khong xac dinh'
    END AS fastest_growing_property_type
FROM nessie.gold.vw_market_trend_analysis
WHERE prev_listing_count IS NOT NULL
ORDER BY snapshot_date, area_name, ward_name, street_name;

-- Xem top loại hình tăng mạnh nhất:

SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,

    apt_change,
    house_change,
    land_change,

    CASE
        WHEN apt_change >= house_change AND apt_change >= land_change THEN 'Can ho / Chung cu'
        WHEN house_change >= apt_change AND house_change >= land_change THEN 'Nha o'
        WHEN land_change >= apt_change AND land_change >= house_change THEN 'Dat'
        ELSE 'Khong xac dinh'
    END AS fastest_growing_property_type
FROM nessie.gold.vw_market_trend_analysis
WHERE prev_listing_count IS NOT NULL
ORDER BY 
    CASE
        WHEN apt_change >= house_change AND apt_change >= land_change THEN apt_change
        WHEN house_change >= apt_change AND house_change >= land_change THEN house_change
        WHEN land_change >= apt_change AND land_change >= house_change THEN land_change
        ELSE 0
    END DESC
LIMIT 20;

-- 5. Xu hướng nguồn cung và giá có cùng chiều hay không?
SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,

    listing_change,
    listing_growth_rate,

    price_per_m2_change,
    price_per_m2_growth_rate,

    supply_price_trend_type
FROM nessie.gold.vw_market_trend_analysis
WHERE prev_listing_count IS NOT NULL
  AND prev_price_per_m2_avg IS NOT NULL
ORDER BY snapshot_date, area_name, ward_name, street_name;

-- Đếm số khu vực theo từng kiểu xu hướng:

SELECT
    snapshot_date,
    supply_price_trend_type,
    COUNT(*) AS area_count
FROM nessie.gold.vw_market_trend_analysis
WHERE prev_listing_count IS NOT NULL
  AND prev_price_per_m2_avg IS NOT NULL
GROUP BY
    snapshot_date,
    supply_price_trend_type
ORDER BY
    snapshot_date,
    area_count DESC;


-----------------------4. . Khu vực nào có dấu hiệu trở thành điểm nóng? --------------
-- vw_hotspot_analysis_base
-- vw_hotspot_analysis

-- Câu 1: Khu vực nào có số lượng tin đăng cao?
SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,
    listing_count,
    unique_seller_count,
    price_per_m2_avg,
    hotspot_score,
    hotspot_level
FROM nessie.gold.vw_hotspot_analysis
ORDER BY listing_count DESC
LIMIT 20;

-- Câu 2: Khu vực nào có tốc độ tăng tin đăng cao?
SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,
    prev_listing_count,
    listing_count,
    listing_change,
    listing_growth_rate,
    hotspot_score,
    hotspot_level
FROM nessie.gold.vw_hotspot_analysis
WHERE listing_growth_rate > 0
ORDER BY listing_growth_rate DESC
LIMIT 20;

-- Câu 3: Khu vực nào có giá/m² tăng nhanh?
SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,
    prev_price_per_m2_avg,
    price_per_m2_avg,
    price_per_m2_change,
    price_per_m2_growth_rate,
    listing_count,
    hotspot_score,
    hotspot_level
FROM nessie.gold.vw_hotspot_analysis
WHERE price_per_m2_growth_rate > 0
ORDER BY price_per_m2_growth_rate DESC
LIMIT 20;

-- Câu 4: Khu vực nào có nhiều người bán tham gia?
SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,
    listing_count,
    unique_seller_count,
    seller_growth_rate,
    price_per_m2_avg,
    hotspot_score,
    hotspot_level
FROM nessie.gold.vw_hotspot_analysis
ORDER BY unique_seller_count DESC
LIMIT 20;
-- Câu 5: Khu vực nào vừa tăng giá, vừa tăng nguồn cung?
SELECT
    snapshot_date,
    area_name,
    ward_name,
    street_name,

    prev_listing_count,
    listing_count,
    listing_change,
    listing_growth_rate,

    prev_price_per_m2_avg,
    price_per_m2_avg,
    price_per_m2_change,
    price_per_m2_growth_rate,

    unique_seller_count,
    supply_price_trend_type,

    hotspot_score,
    hotspot_level
FROM nessie.gold.vw_hotspot_analysis
WHERE listing_change > 0
  AND price_per_m2_change > 0
ORDER BY hotspot_score DESC, listing_growth_rate DESC, price_per_m2_growth_rate DESC
LIMIT 20;

-----------------------5. Metro có ảnh hưởng như thế nào đến thị trường bất động sản? --------------
-- vw_metro_impact_analysis
-- vw_metro_impact_analysis_detail

-- 1. Khu vực gần metro có giá/m² cao hơn không?
SELECT
    metro_group,
    COUNT(*) AS area_count,
    SUM(listing_count) AS total_listing_count,
    AVG(price_per_m2_avg) AS avg_price_per_m2,
    AVG(price_avg) AS avg_price,
    AVG(size_avg) AS avg_size
FROM nessie.gold.vw_metro_impact_analysis_detail
GROUP BY metro_group
ORDER BY avg_price_per_m2 DESC;

-- 2. Số lượng tin đăng gần metro có nhiều hơn không?
SELECT
    metro_group,
    COUNT(*) AS area_count,
    SUM(listing_count) AS total_listing_count,
    AVG(listing_count) AS avg_listing_per_area
FROM nessie.gold.vw_metro_impact_analysis_detail
GROUP BY metro_group
ORDER BY total_listing_count DESC;

-- 3. Loại hình bất động sản nào xuất hiện nhiều quanh metro?
SELECT
    metro_group,

    SUM(apt_count) AS total_apt_count,
    SUM(house_count) AS total_house_count,
    SUM(land_count) AS total_land_count,

    SUM(listing_count) AS total_listing_count,

    CAST(SUM(apt_count) AS DOUBLE) / CAST(SUM(listing_count) AS DOUBLE) AS apt_ratio,
    CAST(SUM(house_count) AS DOUBLE) / CAST(SUM(listing_count) AS DOUBLE) AS house_ratio,
    CAST(SUM(land_count) AS DOUBLE) / CAST(SUM(listing_count) AS DOUBLE) AS land_ratio
FROM nessie.gold.vw_metro_impact_analysis_detail
GROUP BY metro_group
ORDER BY total_listing_count DESC;
-- 4. Các khu vực gần metro nổi bật nhất
SELECT
    area_name,
    ward_name,
    station_name,
    nearest_metro_distance_m,

    listing_count,
    price_per_m2_avg,

    apt_count,
    house_count,
    land_count,

    dominant_property_type
FROM nessie.gold.vw_metro_impact_analysis_detail
WHERE metro_group = 'Gan metro'
ORDER BY listing_count DESC, price_per_m2_avg DESC
LIMIT 20;

-- 5. Mức độ gần metro ảnh hưởng thế nào đến giá và nguồn cung?
SELECT
    metro_proximity_category,
    metro_group,

    COUNT(*) AS area_count,
    SUM(listing_count) AS total_listing_count,
    AVG(listing_count) AS avg_listing_per_area,

    AVG(price_avg) AS avg_price,
    AVG(price_per_m2_avg) AS avg_price_per_m2,
    AVG(size_avg) AS avg_size
FROM nessie.gold.vw_metro_impact_analysis_detail
GROUP BY
    metro_proximity_category,
    metro_group
ORDER BY avg_price_per_m2 DESC;