-- Cek apakah datanya lengkap
SELECT 
  'Raw Tables Completeness Check' as check_type,
  'kasus_tuberkulosis' as table_name,
  COUNT(*) as total_records,
  COUNT(CASE WHEN jumlah_kasus IS NULL THEN 1 END) as null_cases,
  COUNT(CASE WHEN kode_kabupaten_kota IS NULL THEN 1 END) as null_regions
FROM raw.kasus_tuberkulosis

UNION ALL

SELECT 
  'Raw Tables Completeness Check',
  'keberhasilan_pengobatan',
  COUNT(*),
  COUNT(CASE WHEN jumlah_keberhasilan_pengobatan IS NULL THEN 1 END),
  COUNT(CASE WHEN kode_kabupaten_kota IS NULL THEN 1 END)
FROM raw.keberhasilan_pengobatan;

-- cek konsistensi data (nyoba pake CTE)
WITH consistency_check AS (
  SELECT 
    dw.nama_kabupaten_kota,
    SUM(ft.jumlah_kasus) as total_cases,
    SUM(ft.jumlah_sembuh) as total_recovered,
    SUM(ft.jumlah_meninggal) as total_deceased,
    SUM(ft.jumlah_sembuh) + SUM(ft.jumlah_meninggal) as total_outcomes
  FROM staging.fact_tuberkulosis ft
  JOIN staging.dim_wilayah dw ON ft.wilayah_id = dw.wilayah_id
  GROUP BY dw.nama_kabupaten_kota
)
SELECT 
  'Data Consistency Check' as check_type,
  nama_kabupaten_kota,
  total_cases,
  total_recovered,
  total_deceased,
  total_outcomes,
  CASE 
    WHEN total_outcomes > total_cases 
    THEN 'INCONSISTENT: Outcomes > Cases'
    WHEN total_cases > 0 AND total_outcomes = 0
    THEN 'WARNING: No outcomes recorded'
    ELSE 'CONSISTENT'
  END as validation_result
FROM consistency_check
WHERE total_outcomes > total_cases OR (total_cases > 0 AND total_outcomes = 0);

-- ini CTE juga, mau cek outlier
WITH percentile_values AS (
  SELECT 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_kasus) as q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_kasus) as q3
  FROM analytics.tb_by_region
  WHERE tahun = 2019
),
case_statistics AS (
  SELECT 
    nama_kabupaten_kota,
    total_kasus,
    AVG(total_kasus) OVER() as avg_cases,
    STDDEV(total_kasus) OVER() as stddev_cases,
    (SELECT q1 FROM percentile_values) as q1,
    (SELECT q3 FROM percentile_values) as q3
  FROM analytics.tb_by_region
  WHERE tahun = 2019
),
outlier_bounds AS (
  SELECT 
    *,
    q3 + 1.5 * (q3 - q1) as upper_fence,
    q1 - 1.5 * (q3 - q1) as lower_fence,
    avg_cases + 2 * stddev_cases as upper_2sigma,
    avg_cases - 2 * stddev_cases as lower_2sigma
  FROM case_statistics
)
SELECT 
  'Outlier Detection' as check_type,
  nama_kabupaten_kota,
  total_kasus,
  ROUND(avg_cases) as avg_cases,
  ROUND(stddev_cases) as stddev_cases,
  CASE 
    WHEN total_kasus > upper_fence THEN 'HIGH OUTLIER (IQR Method)'
    WHEN total_kasus < lower_fence THEN 'LOW OUTLIER (IQR Method)'
    WHEN total_kasus > upper_2sigma THEN 'HIGH OUTLIER (2-Sigma Method)'
    WHEN total_kasus < lower_2sigma THEN 'LOW OUTLIER (2-Sigma Method)'
    ELSE 'NORMAL'
  END as outlier_status
FROM outlier_bounds
WHERE total_kasus > upper_fence 
   OR total_kasus < lower_fence 
   OR total_kasus > upper_2sigma 
   OR total_kasus < lower_2sigma;

-- Integritas dimensi tabel
SELECT 
  'Dimension Integrity' as check_type,
  'dim_wilayah' as table_name,
  COUNT(*) as total_records,
  COUNT(DISTINCT kode_kabupaten_kota) as unique_regions,
  CASE 
    WHEN COUNT(*) = COUNT(DISTINCT kode_kabupaten_kota) 
    THEN 'UNIQUE CONSTRAINT OK'
    ELSE 'DUPLICATE REGIONS FOUND'
  END as integrity_status
FROM staging.dim_wilayah;

-- Validasi business rulenya
SELECT 
  'Business Rule Validation' as check_type,
  COUNT(*) as total_regions,
  COUNT(CASE WHEN tingkat_kesembuhan > 100 THEN 1 END) as invalid_recovery_rate,
  COUNT(CASE WHEN tingkat_kematian > 100 THEN 1 END) as invalid_mortality_rate,
  COUNT(CASE WHEN tingkat_kesembuhan < 0 THEN 1 END) as negative_recovery_rate,
  COUNT(CASE WHEN tingkat_kematian < 0 THEN 1 END) as negative_mortality_rate
FROM analytics.tb_by_region;

--
SELECT 
  'ETL Pipeline Validation' as check_type,
  'Record Count Comparison' as validation_aspect,
  (SELECT COUNT(DISTINCT kode_kabupaten_kota) FROM raw.kasus_tuberkulosis) as raw_regions,
  (SELECT COUNT(*) FROM staging.dim_wilayah) as staging_regions,
  (SELECT COUNT(DISTINCT kode_kabupaten_kota) FROM analytics.tb_by_region) as analytics_regions,
  CASE 
    WHEN (SELECT COUNT(DISTINCT kode_kabupaten_kota) FROM raw.kasus_tuberkulosis) = 
         (SELECT COUNT(*) FROM staging.dim_wilayah)
    THEN 'PIPELINE CONSISTENT'
    ELSE 'DATA LOSS DETECTED'
  END as pipeline_status;