-- schema
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA analytics;


CREATE TABLE raw.kasus_penyakit_tuberkulosis (
  id INT,
  kode_provinsi VARCHAR(10),
  nama_provinsi TEXT,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  jumlah_kasus INT,
  satuan TEXT,
  tahun INT
);

CREATE TABLE raw.kasus_tuberkulosis (
  id INT,
  kode_provinsi VARCHAR(10),
  nama_provinsi TEXT,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  jenis_kelamin TEXT,
  jumlah_kasus INT,
  satuan TEXT,
  tahun INT
);

CREATE TABLE raw.keberhasilan_pengobatan (
  id INT,
  kode_provinsi VARCHAR(10),
  nama_provinsi TEXT,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  jenis_kelamin TEXT,
  jumlah_keberhasilan_pengobatan INT,
  satuan TEXT,
  tahun INT
);

CREATE TABLE raw.kematian (
  id INT,
  kode_provinsi VARCHAR(10),
  nama_provinsi TEXT,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  jumlah_kematian INT,
  satuan TEXT,
  tahun INT
);

CREATE TABLE raw.kesembuhan_terkonfirmasi (
  id INT,
  kode_provinsi VARCHAR(10),
  nama_provinsi TEXT,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  jenis_kelamin TEXT,
  jumlah_kesembuhan INT,
  satuan TEXT,
  tahun INT
);

CREATE TABLE raw.terduga (
  id INT,
  kode_provinsi VARCHAR(10),
  nama_provinsi TEXT,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  jumlah_terduga INT,
  satuan TEXT,
  tahun INT
);

CREATE TABLE raw.tuberkulosis_anak (
  id INT,
  kode_provinsi VARCHAR(10),
  nama_provinsi TEXT,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  jumlah_anak INT,
  satuan TEXT,
  tahun INT
);

CREATE TABLE raw.pengobatan (
  id INT,
  kode_provinsi VARCHAR(10),
  nama_provinsi TEXT,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  jenis_kelamin TEXT,
  jumlah_pengobatan INT,
  satuan TEXT,
  tahun INT
);

CREATE TABLE raw.jenis_penyakit (
  id INT,
  kode_provinsi VARCHAR(10),
  nama_provinsi TEXT,
  kode_kabupaten_kota VARCHAR(10),
  nama_kabupaten_kota TEXT,
  jenis_penyakit TEXT,
  jumlah_kasus INT,
  satuan TEXT,
  tahun INT
);