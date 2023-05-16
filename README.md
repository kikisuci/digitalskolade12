# digitalskolade12
this repository for Digitalskola batch 12 final project

Project template dapat dilihat dan di fork di:
https://github.com/saungkertas/digitalskolade12

1. Buatlah arsitektur end to end dari source (postgresql) hingga ke datawarehouse(Snowflake)
2. Buatlah scheduler untuk ingest data dari Postgresql ke datawarehouse(Snowflake), dapat menggunakan ETL seperti (airbyte, python script, other ETL tools)
3. Buatlah scheduler menggunakan airflow untuk membentuk table Datamart.
4. Buatlah datamart view berikut:
 - Gross revenue total per hari
 - Gross Revenue per product per bulan
 - Jumlah total pembelian per product per bulan
 - Jumlah total pembelian per kategori product per bulan
 - Jumlah total pembelian per negara per bulan
4. Buatlah dashboard chart dari poin 3. (dapat menggunakan Snowsight atau Looker Data Studio)
 - Gross revenue total per hari (line chart)
 - Gross Revenue per product per bulan (bar chart)
 - Jumlah total pembelian per product per bulan (bar chart, top 10 highest)
 - Jumlah total pembelian per kategori product per bulan (bar chart, top 10 highest)
 - Jumlah total pembelian per negara per bulan (map chart)
5. Setelah berhasil, masukkan python code dan airflow dags di dalam directory dags di repository ini airflow/dags/
6. Presentasikan hasil pengerjaan, termasuk arsitektur pipeline, step pengerjaan, hingga kendala yang dialami selama pengerjaan. 

Catatan:
- dataset yang digunakan adalah northwind dataset (https://raw.githubusercontent.com/pthom/northwind_psql/master/ER.png)
- fork project https://github.com/saungkertas/digitalskolade12 dan submit PR jika sudah selesai
- script dags ditaruh di bagian airflow/dags/
- script dapat dijalankan manual terlebih dahulu di server sebelum disubmit(example:python3 ingest_orders.py 2022-08-04)
- untuk develop script atau coba2, bisa dilakukan di directory /root/coba2.
- folder dags tiap hari nya akan di refresh dan di pull dari repository terakhir.
- silakan japri jika ada perubahan script/dags, akan diapprove sore hari tiap harinya.
- Fact table menggunakan daily run, sedangkan dimension table hanya perlu init run (sekali run saja untuk mengambil semua data)
- setiap table yang dicreate, beri prefix <nama>_nama_table
- IP database dan VM akan diinfokan di grup atau classroom.
