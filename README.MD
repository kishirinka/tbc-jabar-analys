# West Java Tuberculosis Data Analysis

This repository contains analysis of tuberculosis (TB) cases data in West Java Province using data from the West Java Open Data Portal.

## Data Source

Data in this repository is sourced from **West Java Province Open Data Portal**:
- **Website**: [opendata.jabarprov.go.id](https://opendata.jabarprov.go.id/)
- **Main Portal**: [data.jabarprov.go.id](https://data.jabarprov.go.id/)

## Data Structure

This repository has the following structure:

### Data (`/data/`)
Tuberculosis datasets available in CSV format:

| File | Description |
|------|-------------|
| `jenis_penyakit.csv` | Various disease types data including tuberculosis by regency/city |
| `kasus_penyakit_tuberkulosis.csv` | Specific tuberculosis disease cases data |
| `kasus_tuberkulosis.csv` | Tuberculosis cases data by gender and region |
| `keberhasilan_pengobatan_tuberkulos.csv` | Tuberculosis treatment success rate data |
| `kematian_pengobatan_tuberkulosis.csv` | Death from tuberculosis during treatment data |
| `kesembuhan_tuberkulosis_paru_terkonfirmasi.csv` | Recovery data of confirmed pulmonary tuberculosis patients |
| `terduga_tuberkulosis.csv` | Suspected tuberculosis cases data |
| `tuberkulosis_anak.csv` | Tuberculosis cases in children data |
| `tuberkulosis_diobati.csv` | Tuberculosis patients under treatment data |

### SQL (`/sql/`)
- `import_data.sql` - SQL script to create database structure and import data

## Dataset Information

### Data Coverage
- **Region**: West Java Province (all regencies/cities)
- **Province Code**: 32
- **Year**: 2019 (main data)
- **Categories**: 
  - By gender (Male/Female)
  - By administrative region (regency/city)
  - By age category (children specific)

### Data Format
- **Format**: CSV (Comma Separated Values)
- **Encoding**: UTF-8
- **Unit**: Person/Cases
- **General structure**:
  - `id`: Unique record ID
  - `kode_provinsi`: Province code (32)
  - `nama_provinsi`: JAWA BARAT (West Java)
  - `kode_kabupaten_kota`: Regency/city code
  - `nama_kabupaten_kota`: Region name
  - `jenis_kelamin`: LAKI-LAKI/PEREMPUAN (Male/Female)
  - `jumlah_*`: Number of cases/success/deaths
  - `satuan`: ORANG/KASUS (Person/Cases)
  - `tahun`: Data year

## 🛠 Usage

### Prerequisites
- Database system (PostgreSQL/MySQL recommended)
- Python/R for data analysis
- Visualization tools (Matplotlib, Seaborn, Tableau, etc.)

### Import Data to Database
```sql
-- Run SQL script to create database structure
psql -d database_name -f sql/import_data.sql

-- Import CSV data to created tables
COPY raw.kasus_tuberkulosis FROM 'data/kasus_tuberkulosis.csv' DELIMITER ',' CSV HEADER;
-- Repeat for other datasets...
```

## Data Contact
For further inquiries about the data:
- **Email**: data@jabarprov.go.id  
- **WhatsApp**: +62 821-2603-0038
- **Website**: [opendata.jabarprov.go.id](https://opendata.jabarprov.go.id/)

## License
This data follows the terms of use from West Java Province Open Data Portal. Please refer to [Terms of Use](https://opendata.jabarprov.go.id/id/ketentuan-penggunaan) and [Privacy Policy](https://opendata.jabarprov.go.id/id/kebijakan-privasi) for more information.

---

**Last Updated**: January 2026  
**Data Source**: West Java Province Open Data