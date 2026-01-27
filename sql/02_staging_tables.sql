CREATE TABLE staging.wilayah (
  wilayah_id SERIAL PRIMARY KEY,
  kode_provinsi VARCHAR(10),
  nama_provinsi TEXT,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  UNIQUE(kode_kabupaten_kota)
);

CREATE TABLE staging.waktu (
  waktu_id SERIAL PRIMARY KEY,
  tahun INT,
  kuartal INT,
  semester INT,
  UNIQUE(tahun)
);

CREATE TABLE staging.demografi (
  demografi_id SERIAL PRIMARY KEY,
  jenis_kelamin VARCHAR(20),
  kategori_usia VARCHAR(20),
  UNIQUE(jenis_kelamin, kategori_usia)
);

CREATE TABLE staging.tuberkulosis (
  tb_id SERIAL PRIMARY KEY,
  wilayah_id INT REFERENCES staging.wilayah(wilayah_id),
  waktu_id INT REFERENCES staging.waktu(waktu_id),
  demografi_id INT REFERENCES staging.demografi(demografi_id),
  jumlah_kasus INT DEFAULT 0,
  jumlah_terduga INT DEFAULT 0,
  jumlah_diobati INT DEFAULT 0,
  jumlah_sembuh INT DEFAULT 0,
  jumlah_berhasil_diobati INT DEFAULT 0,
  jumlah_meninggal INT DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.dim_wilayah (
  wilayah_id SERIAL PRIMARY KEY,
  kode_provinsi VARCHAR(10),
  nama_provinsi TEXT,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  UNIQUE(kode_kabupaten_kota)
);

CREATE TABLE staging.dim_waktu (
  waktu_id SERIAL PRIMARY KEY,
  tahun INT,
  kuartal INT,
  semester INT,
  UNIQUE(tahun)
);

CREATE TABLE staging.dim_demografi (
  demografi_id SERIAL PRIMARY KEY,
  jenis_kelamin VARCHAR(20),
  kategori_usia VARCHAR(20),
  UNIQUE(jenis_kelamin, kategori_usia)
);

CREATE TABLE staging.fact_tuberkulosis (
  tb_id SERIAL PRIMARY KEY,
  wilayah_id INT REFERENCES staging.dim_wilayah(wilayah_id),
  waktu_id INT REFERENCES staging.dim_waktu(waktu_id),
  demografi_id INT REFERENCES staging.dim_demografi(demografi_id),
  jumlah_kasus INT DEFAULT 0,
  jumlah_terduga INT DEFAULT 0,
  jumlah_diobati INT DEFAULT 0,
  jumlah_sembuh INT DEFAULT 0,
  jumlah_berhasil_diobati INT DEFAULT 0,
  jumlah_meninggal INT DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);