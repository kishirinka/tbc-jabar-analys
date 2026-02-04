-- ETL Analytics
-- Ringkasan TB perwilayah dan tahun
WITH fact_filtered AS (
  SELECT 
    ft.wilayah_id,
    ft.waktu_id,
    ft.jumlah_kasus,
    ft.jumlah_sembuh,
    ft.jumlah_meninggal
  FROM staging.fact_tuberkulosis ft
  WHERE ft.jumlah_kasus > 0
),
dimension_joined AS (
  SELECT 
    dw.kode_kabupaten_kota,
    dw.nama_kabupaten_kota,
    dt.tahun,
    ff.jumlah_kasus,
    ff.jumlah_sembuh,
    ff.jumlah_meninggal
  FROM fact_filtered ff
  INNER JOIN staging.dim_waktu dt ON ff.waktu_id = dt.waktu_id
  INNER JOIN staging.dim_wilayah dw ON ff.wilayah_id = dw.wilayah_id
  WHERE dt.tahun >= 2015  
),
regional_aggregates AS (
  SELECT 
    kode_kabupaten_kota,
    nama_kabupaten_kota,
    tahun,
    SUM(jumlah_kasus) as total_kasus,
    SUM(jumlah_sembuh) as total_sembuh,
    SUM(jumlah_meninggal) as total_meninggal
  FROM dimension_joined
  GROUP BY kode_kabupaten_kota, nama_kabupaten_kota, tahun
  HAVING SUM(jumlah_kasus) > 10  
),

-- persentase kesembuhan dan kematian
calculated_metrics AS (
  SELECT 
    *,
    ROUND((total_sembuh::DECIMAL / NULLIF(total_kasus, 0)) * 100, 2) as tingkat_kesembuhan,
    ROUND((total_meninggal::DECIMAL / NULLIF(total_kasus, 0)) * 100, 2) as tingkat_kematian
  FROM regional_aggregates
),

  -- Ranking wilayah berdasarkan jumlah kasus
final_with_ranking AS (
  SELECT 
    *,
    RANK() OVER (PARTITION BY tahun ORDER BY total_kasus DESC) as rank_by_cases
  FROM calculated_metrics
)
INSERT INTO analytics.tb_by_region
SELECT 
  ROW_NUMBER() OVER() as region_id,
  kode_kabupaten_kota,
  nama_kabupaten_kota,
  tahun,
  total_kasus,
  total_sembuh,
  total_meninggal,
  COALESCE(tingkat_kesembuhan, 0) as tingkat_kesembuhan,
  COALESCE(tingkat_kematian, 0) as tingkat_kematian,
  rank_by_cases,
  CURRENT_TIMESTAMP as created_at
FROM final_with_ranking;

-- ringkasan berdasarkan gender
WITH gender_totals AS (
  SELECT 
    dd.jenis_kelamin,
    dt.tahun,
    ft.waktu_id,
    SUM(ft.jumlah_kasus) as total_kasus,
    SUM(ft.jumlah_sembuh) as total_sembuh
  FROM staging.fact_tuberkulosis ft
  INNER JOIN staging.dim_waktu dt ON ft.waktu_id = dt.waktu_id
  INNER JOIN staging.dim_demografi dd ON ft.demografi_id = dd.demografi_id
  WHERE ft.jumlah_kasus > 0 
    AND dd.kategori_usia = 'DEWASA'
  GROUP BY dd.jenis_kelamin, dt.tahun, ft.waktu_id
),
yearly_totals AS (
  SELECT 
    waktu_id,
    SUM(total_kasus) as yearly_total_cases
  FROM gender_totals
  GROUP BY waktu_id
)
INSERT INTO analytics.tb_by_gender
SELECT 
  ROW_NUMBER() OVER() as gender_id,
  gt.jenis_kelamin,
  gt.tahun,
  gt.total_kasus,
  gt.total_sembuh,
  ROUND((gt.total_kasus::DECIMAL / yt.yearly_total_cases) * 100, 2) as persentase_kasus,
  CASE 
    WHEN gt.total_kasus > 0 
    THEN ROUND((gt.total_sembuh::DECIMAL / gt.total_kasus) * 100, 2)
    ELSE 0 
  END as persentase_sembuh,
  CURRENT_TIMESTAMP as created_at
FROM gender_totals gt
INNER JOIN yearly_totals yt ON gt.waktu_id = yt.waktu_id;

-- KPI
WITH kpi_calculations AS (
  SELECT 
    (SELECT SUM(jumlah_kasus) FROM staging.fact_tuberkulosis) as total_kasus_provinsi,
    (SELECT AVG(tingkat_kesembuhan) FROM analytics.tb_by_region WHERE tahun = 2019) as avg_tingkat_sembuh,
    (SELECT AVG(tingkat_kematian) FROM analytics.tb_by_region WHERE tahun = 2019) as avg_tingkat_mati,
    (SELECT COUNT(*) FROM analytics.tb_by_region WHERE total_kasus > 3000 AND tahun = 2019) as count_high_burden
)
INSERT INTO analytics.tb_kpi_dashboard 
SELECT * FROM (VALUES 
  (1, 'Total Kasus TB Provinsi', 
   (SELECT total_kasus_provinsi FROM kpi_calculations), 
   0, 0, '2019', 'ACTUAL', CURRENT_TIMESTAMP),
  
  (2, 'Rata-rata Tingkat Kesembuhan', 
   (SELECT avg_tingkat_sembuh FROM kpi_calculations), 
   85, 0, '2019', 'MONITORING', CURRENT_TIMESTAMP),
   
  (3, 'Rata-rata Tingkat Kematian', 
   (SELECT avg_tingkat_mati FROM kpi_calculations), 
   5, 0, '2019', 'MONITORING', CURRENT_TIMESTAMP),
   
  (4, 'Jumlah Kabupaten High Burden', 
   (SELECT count_high_burden FROM kpi_calculations), 
   10, 0, '2019', 'ALERT', CURRENT_TIMESTAMP)
) as kpi_data(kpi_id, metric_name, metric_value, target_value, achievement_rate, periode, status, updated_at);

UPDATE analytics.tb_kpi_dashboard 
SET achievement_rate = CASE 
    WHEN metric_name = 'Rata-rata Tingkat Kesembuhan' 
    THEN ROUND((metric_value / target_value) * 100, 2)
    WHEN metric_name = 'Rata-rata Tingkat Kematian' 
    THEN ROUND(((target_value - metric_value) / target_value) * 100, 2)
    ELSE 100
END
WHERE target_value > 0;

-- Area dengan beban tinggi
WITH burden_candidates AS (
  SELECT 
    kode_kabupaten_kota,
    nama_kabupaten_kota,
    tahun,
    total_kasus
  FROM analytics.tb_by_region
  WHERE tahun = 2019        
    AND total_kasus > 100 
),
burden_categories AS (
  SELECT 
    *,
    CASE 
      WHEN total_kasus > 5000 THEN 'VERY HIGH'
      WHEN total_kasus > 3000 THEN 'HIGH'
      WHEN total_kasus > 1000 THEN 'MEDIUM'
      ELSE 'LOW'
    END as burden_category,
    CASE 
      WHEN total_kasus > 5000 THEN 1
      WHEN total_kasus > 3000 THEN 2
      WHEN total_kasus > 1000 THEN 3
      ELSE 4
    END as priority_level
  FROM burden_candidates
)
INSERT INTO analytics.tb_high_burden_areas
SELECT 
  ROW_NUMBER() OVER() as burden_id,
  kode_kabupaten_kota,
  nama_kabupaten_kota,
  tahun,
  ROUND((total_kasus::DECIMAL / 100000) * 100, 2) as incidence_rate,
  100000 as population_estimate,
  burden_category,
  priority_level,
  CURRENT_TIMESTAMP as created_at
FROM burden_categories
WHERE burden_category IN ('HIGH', 'VERY HIGH')  
ORDER BY priority_level, total_kasus DESC;

-- ETL Age Analysis (DEWASA vs ANAK)
WITH age_totals AS (
  SELECT 
    dd.kategori_usia,
    dt.tahun,
    SUM(ft.jumlah_kasus) as total_kasus
  FROM staging.fact_tuberkulosis ft
  INNER JOIN staging.dim_waktu dt ON ft.waktu_id = dt.waktu_id
  INNER JOIN staging.dim_demografi dd ON ft.demografi_id = dd.demografi_id
  WHERE ft.jumlah_kasus > 0
  GROUP BY dd.kategori_usia, dt.tahun
),
yearly_age_total AS (
  SELECT 
    tahun,
    SUM(total_kasus) as yearly_total_cases
  FROM age_totals
  GROUP BY tahun
)
INSERT INTO analytics.tb_by_age
SELECT 
  ROW_NUMBER() OVER() as age_id,
  at.kategori_usia,
  at.tahun,
  at.total_kasus,
  ROUND((at.total_kasus::DECIMAL / yt.yearly_total_cases) * 100, 2) as persentase_kasus,
  CURRENT_TIMESTAMP as created_at
FROM age_totals at
INNER JOIN yearly_age_total yt ON at.tahun = yt.tahun;