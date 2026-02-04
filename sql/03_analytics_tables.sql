-- Dimension dan Fact Tables untuk Data Warehouse
-- Pre-aggregated Analytics Tables
CREATE TABLE analytics.tb_by_region (
  region_id SERIAL PRIMARY KEY,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  tahun INT,
  total_kasus INT,
  total_sembuh INT,
  total_meninggal INT,
  tingkat_kesembuhan DECIMAL(5,2),
  tingkat_kematian DECIMAL(5,2),
  rank_by_cases INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE analytics.tb_by_gender (
  gender_id SERIAL PRIMARY KEY,
  jenis_kelamin VARCHAR(20),
  tahun INT,
  total_kasus INT,
  total_sembuh INT,
  persentase_kasus DECIMAL(5,2),
  persentase_sembuh DECIMAL(5,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE analytics.tb_trend (
  trend_id SERIAL PRIMARY KEY,
  tahun INT,
  bulan INT,
  kode_kabupaten_kota VARCHAR(10),
  total_kasus INT,
  total_sembuh INT,
  mom_growth_rate DECIMAL(5,2),
  yoy_growth_rate DECIMAL(5,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE analytics.tb_kpi_dashboard (
  kpi_id SERIAL PRIMARY KEY,
  metric_name VARCHAR(100),
  metric_value DECIMAL(15,2),
  target_value DECIMAL(15,2),
  achievement_rate DECIMAL(5,2),
  periode VARCHAR(20),
  status VARCHAR(20),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE analytics.tb_high_burden_areas (
  burden_id SERIAL PRIMARY KEY,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  tahun INT,
  incidence_rate DECIMAL(10,2),
  population_estimate INT,
  burden_category VARCHAR(20),
  priority_level INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE analytics.tb_by_age (
  age_id SERIAL PRIMARY KEY,
  kategori_usia VARCHAR(20),
  tahun INT,
  total_kasus INT DEFAULT 0,
  persentase_kasus DECIMAL(5,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);