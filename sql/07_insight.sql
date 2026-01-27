-- Wilayah dengan kasus tinggi
WITH region_filtered AS (
  SELECT 
    nama_kabupaten_kota,
    total_kasus,
    tingkat_kesembuhan,
    rank_by_cases
  FROM analytics.tb_by_region 
  WHERE tahun = 2019
    AND total_kasus > 500
    AND tingkat_kesembuhan IS NOT NULL
)
SELECT 
  nama_kabupaten_kota,
  total_kasus,
  tingkat_kesembuhan,
  rank_by_cases,
  CASE 
    WHEN tingkat_kesembuhan >= 85 THEN 'Excellent'
    WHEN tingkat_kesembuhan >= 70 THEN 'Good'
    ELSE 'Needs Improvement'
  END AS performance_category
FROM region_filtered
ORDER BY total_kasus DESC
LIMIT 10;

-- Analisis kasus tbc ini berdasarkan gender
WITH gender_data AS (
  SELECT 
    jenis_kelamin,
    total_kasus,
    total_sembuh,
    persentase_kasus
  FROM analytics.tb_by_gender
  WHERE tahun = 2019
    AND total_kasus > 0
)
SELECT 
  jenis_kelamin,
  ROUND(total_kasus / 1000.0, 1) AS total_kasus_ribuan,
  persentase_kasus,
  ROUND((total_sembuh::DECIMAL / total_kasus) * 100, 2) AS recovery_rate,
  CASE 
    WHEN (total_sembuh::DECIMAL / total_kasus) * 100 >= 80 
    THEN 'Target Met'
    ELSE 'Below Target'
  END AS target_status
FROM gender_data
ORDER BY total_kasus DESC;

-- Alur penanganan TB per wilayah
WITH tb_summary AS (
  -- Rekap data TB per wilayah
  SELECT 
    ft.wilayah_id,
    SUM(ft.jumlah_terduga) AS terduga,
    SUM(ft.jumlah_kasus) AS terdiagnosis,
    SUM(ft.jumlah_diobati) AS diobati,
    SUM(ft.jumlah_sembuh) AS sembuh
  FROM staging.fact_tuberkulosis ft
  JOIN staging.dim_waktu dw ON ft.waktu_id = dw.waktu_id
  WHERE dw.tahun = 2019
    AND ft.jumlah_terduga > 0
  GROUP BY ft.wilayah_id
  HAVING SUM(ft.jumlah_terduga) >= 50
)
SELECT 
  w.nama_kabupaten_kota,
  t.terduga,
  t.terdiagnosis,
  t.diobati,
  t.sembuh,
  ROUND((t.terdiagnosis::DECIMAL / t.terduga) * 100, 1) AS detection_rate,
  ROUND((t.diobati::DECIMAL / t.terdiagnosis) * 100, 1) AS treatment_rate,
  ROUND((t.sembuh::DECIMAL / t.diobati) * 100, 1) AS success_rate,
  CASE 
    WHEN (t.terdiagnosis::DECIMAL / t.terduga) * 100 >= 70
     AND (t.diobati::DECIMAL / t.terdiagnosis) * 100 >= 90
     AND (t.sembuh::DECIMAL / t.diobati) * 100 >= 85
    THEN 'Optimal'
    WHEN (t.terdiagnosis::DECIMAL / t.terduga) * 100 < 50
      OR (t.diobati::DECIMAL / t.terdiagnosis) * 100 < 70
    THEN 'Critical'
    ELSE 'Moderate'
  END AS performance_status
FROM tb_summary t
JOIN staging.dim_wilayah w ON t.wilayah_id = w.wilayah_id
ORDER BY detection_rate DESC;

-- Tren bulanan kasus TB (estimasi)
WITH monthly_data AS (
  SELECT 
    nama_kabupaten_kota,
    tahun,
    generate_series(1, 12) AS bulan,
    ROUND(
      (total_kasus / 12.0) *
      (1 + 0.1 * SIN(generate_series(1, 12) * PI() / 6))
    ) AS kasus_bulanan
  FROM analytics.tb_by_region
  WHERE tahun = 2019
    AND total_kasus > 1000
),

trend_data AS (
  SELECT 
    nama_kabupaten_kota,
    bulan,
    kasus_bulanan,
    LAG(kasus_bulanan) OVER (
      PARTITION BY nama_kabupaten_kota 
      ORDER BY bulan
    ) AS kasus_bulan_lalu,

    AVG(kasus_bulanan) OVER (
      PARTITION BY nama_kabupaten_kota
      ORDER BY bulan
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )::numeric AS rata_rata_3_bulan
  FROM monthly_data
)

SELECT 
  nama_kabupaten_kota,
  bulan,
  kasus_bulanan,
  kasus_bulan_lalu,
  ROUND(rata_rata_3_bulan, 1) AS rata_rata_3_bulan,
    CASE 
      WHEN kasus_bulan_lalu > 0
      THEN ROUND(
        (
          (kasus_bulanan - kasus_bulan_lalu) * 100.0
          / kasus_bulan_lalu
        )::numeric,
        1
      )
      ELSE 0
    END AS pertumbuhan_bulanan
FROM trend_data
WHERE bulan >= 3
ORDER BY nama_kabupaten_kota, bulan;
