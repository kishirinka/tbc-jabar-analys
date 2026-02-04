-- Membuat table yang berisi metrik utama TB per wilayah, waktu, dan demografi (Star schema -> fact dan dimensi tables)

-- Isi tabel dimensi wilayah (data provinsi dan kabupaten/kota unik)
INSERT INTO staging.dim_wilayah (kode_provinsi, nama_provinsi, kode_kabupaten_kota, nama_kabupaten_kota)
SELECT DISTINCT 
  kode_provinsi, 
  nama_provinsi, 
  kode_kabupaten_kota, 
  nama_kabupaten_kota
FROM raw.kasus_tuberkulosis
WHERE kode_kabupaten_kota IS NOT NULL 
  AND nama_kabupaten_kota IS NOT NULL
ON CONFLICT (kode_kabupaten_kota) DO NOTHING;

-- Isi tabel dimensi waktu (pakai kuartal dan semester)
INSERT INTO staging.dim_waktu (tahun, kuartal, semester)
SELECT DISTINCT 
  tahun,
  CASE 
    WHEN tahun % 4 = 1 THEN 1 
    WHEN tahun % 4 = 2 THEN 2 
    WHEN tahun % 4 = 3 THEN 3 
    ELSE 4 
  END as kuartal,
  CASE WHEN tahun % 2 = 1 THEN 1 ELSE 2 END as semester
FROM raw.kasus_tuberkulosis
WHERE tahun IS NOT NULL
  AND tahun >= 2010 
ON CONFLICT (tahun) DO NOTHING;

-- Isi tabel dimensi demografi
INSERT INTO staging.dim_demografi (jenis_kelamin, kategori_usia)
VALUES 
  ('LAKI-LAKI', 'DEWASA'),
  ('PEREMPUAN', 'DEWASA'),
  ('LAKI-LAKI', 'ANAK'),
  ('PEREMPUAN', 'ANAK')
ON CONFLICT (jenis_kelamin, kategori_usia) DO NOTHING;

-- Isi fact table
WITH base_cases AS (
  SELECT 
    kode_kabupaten_kota,
    tahun,
    jenis_kelamin,
    jumlah_kasus
  FROM raw.kasus_tuberkulosis
  WHERE jumlah_kasus > 0 
    AND kode_kabupaten_kota IS NOT NULL
    AND tahun BETWEEN 2010 AND 2025 
    AND jenis_kelamin IN ('LAKI-LAKI', 'PEREMPUAN')
),
enriched_with_dimensions AS (
  -- JOIN dengan dimensi tables
  SELECT 
    bc.*,
    dw.wilayah_id,
    dt.waktu_id,
    dd.demografi_id
  FROM base_cases bc
  INNER JOIN staging.dim_wilayah dw 
    ON bc.kode_kabupaten_kota = dw.kode_kabupaten_kota
  INNER JOIN staging.dim_waktu dt 
    ON bc.tahun = dt.tahun  
  INNER JOIN staging.dim_demografi dd 
    ON bc.jenis_kelamin = dd.jenis_kelamin 
    AND dd.kategori_usia = 'DEWASA'
),

filtered_terduga AS (
  SELECT kode_kabupaten_kota, tahun, jumlah_terduga
  FROM raw.terduga
  WHERE jumlah_terduga > 0
),
filtered_diobati AS (
  SELECT kode_kabupaten_kota, tahun, jenis_kelamin, jumlah_pengobatan
  FROM raw.pengobatan
  WHERE jumlah_pengobatan > 0
),
filtered_sembuh AS (
  SELECT kode_kabupaten_kota, tahun, jenis_kelamin, jumlah_kesembuhan
  FROM raw.kesembuhan_terkonfirmasi 
  WHERE jumlah_kesembuhan > 0
),
filtered_berhasil AS (
  SELECT kode_kabupaten_kota, tahun, jenis_kelamin, jumlah_keberhasilan_pengobatan
  FROM raw.keberhasilan_pengobatan 
  WHERE jumlah_keberhasilan_pengobatan > 0
),
filtered_meninggal AS (
  SELECT kode_kabupaten_kota, tahun, jumlah_kematian
  FROM raw.kematian
  WHERE jumlah_kematian > 0
)

-- Insert data ke fact table
INSERT INTO staging.fact_tuberkulosis 
(wilayah_id, waktu_id, demografi_id, jumlah_kasus, jumlah_terduga, jumlah_diobati, jumlah_sembuh, jumlah_berhasil_diobati, jumlah_meninggal)
SELECT 
  ewd.wilayah_id,
  ewd.waktu_id,
  ewd.demografi_id,
  ewd.jumlah_kasus,
  COALESCE(td.jumlah_terduga, 0),
  COALESCE(tdo.jumlah_pengobatan, 0),
  COALESCE(ks.jumlah_kesembuhan, 0),
  COALESCE(kb.jumlah_keberhasilan_pengobatan, 0),
  COALESCE(km.jumlah_kematian, 0)
FROM enriched_with_dimensions ewd

LEFT JOIN filtered_terduga td 
  ON ewd.kode_kabupaten_kota = td.kode_kabupaten_kota 
  AND ewd.tahun = td.tahun
LEFT JOIN filtered_diobati tdo 
  ON ewd.kode_kabupaten_kota = tdo.kode_kabupaten_kota 
  AND ewd.tahun = tdo.tahun 
  AND ewd.jenis_kelamin = tdo.jenis_kelamin
LEFT JOIN filtered_sembuh ks 
  ON ewd.kode_kabupaten_kota = ks.kode_kabupaten_kota 
  AND ewd.tahun = ks.tahun 
  AND ewd.jenis_kelamin = ks.jenis_kelamin
LEFT JOIN filtered_berhasil kb 
  ON ewd.kode_kabupaten_kota = kb.kode_kabupaten_kota 
  AND ewd.tahun = kb.tahun 
  AND ewd.jenis_kelamin = kb.jenis_kelamin
LEFT JOIN filtered_meninggal km 
  ON ewd.kode_kabupaten_kota = km.kode_kabupaten_kota 
  AND ewd.tahun = km.tahun

UNION ALL
-- Add Children Data
SELECT 
  dw.wilayah_id,
  dt.waktu_id,
  dd.demografi_id,
  ta.jumlah_anak as jumlah_kasus,
  0 as jumlah_terduga, 
  0 as jumlah_diobati,
  0 as jumlah_sembuh,
  0 as jumlah_berhasil_diobati,
  0 as jumlah_meninggal
FROM raw.tuberkulosis_anak ta
INNER JOIN staging.dim_wilayah dw ON ta.kode_kabupaten_kota = dw.kode_kabupaten_kota
INNER JOIN staging.dim_waktu dt ON ta.tahun = dt.tahun  
INNER JOIN staging.dim_demografi dd ON dd.kategori_usia = 'ANAK'
WHERE ta.jumlah_anak > 0 
  AND ta.tahun BETWEEN 2010 AND 2025;