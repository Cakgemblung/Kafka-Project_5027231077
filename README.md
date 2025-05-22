# Kafka-Project_5027231077
# Proyek Monitoring Gudang dengan Kafka dan PySpark

Proyek ini mendemonstrasikan sistem pemantauan kondisi gudang secara real-time menggunakan Apache Kafka untuk streaming data sensor dan Apache Spark (PySpark) untuk pemrosesan dan analisis data. Sistem ini memantau suhu dan kelembaban dari beberapa gudang dan memberikan peringatan jika kondisi melebihi ambang batas tertentu, serta peringatan kritis jika kedua kondisi (suhu tinggi dan kelembaban tinggi) terjadi bersamaan.

## Fitur Utama

*   **Streaming Data Real-time:** Menggunakan Kafka untuk mengirimkan data sensor suhu dan kelembaban secara berkelanjutan.
*   **Simulasi Sensor:** Dua producer Python mensimulasikan pengiriman data suhu dan kelembaban dari tiga gudang (G1, G2, G3).
*   **Pemrosesan Data dengan PySpark:** Consumer PySpark membaca data dari topik Kafka.
*   **Filtering & Peringatan Individual:**
    *   Memberikan peringatan jika suhu gudang > 80°C.
    *   Memberikan peringatan jika kelembaban gudang > 70%.
*   **Analisis Gabungan (Stream Join):** Menggabungkan stream data suhu dan kelembaban berdasarkan `gudang_id` dan window waktu (10 detik) untuk analisis komprehensif.
*   **Peringatan Kritis Gabungan:** Memberikan peringatan kritis jika suhu > 80°C DAN kelembaban > 70% terdeteksi pada gudang yang sama dalam window waktu yang ditentukan.
*   **Status Kondisi Gudang:** Menampilkan status kondisi setiap gudang (Aman, Suhu Tinggi, Kelembaban Tinggi, atau Bahaya Kritis).

## Struktur Proyek

```
DOCKERKAFKA/
├── app/
│   ├── producer_suhu_gudang.py         # Producer untuk data suhu
│   ├── producer_kelembaban_gudang.py   # Producer untuk data kelembaban
│   └── consumer_gudang_pyspark.py    # Consumer PySpark untuk memproses data
├── DockerKafka_venv/                     # Virtual environment Python
├── .gitignore
├── pyvenv.cfg
├── requirements.txt                      # (Disarankan) Daftar dependensi Python
└── README.md                             # File ini
```

## Teknologi yang Digunakan

*   **Apache Kafka:** Platform streaming data terdistribusi.
*   **Apache Spark (PySpark):** Mesin analisis terpadu untuk pemrosesan data skala besar.
*   **Python:** Bahasa pemrograman utama untuk producer dan consumer.
*   **Java Development Kit (JDK):** Diperlukan untuk menjalankan Kafka dan Zookeeper.
*   **Zookeeper:** Layanan koordinasi untuk Kafka.

## Persyaratan Sistem

*   Java Development Kit (JDK) versi 8 atau lebih tinggi (JDK 11 atau 17 direkomendasikan).
*   Apache Kafka (versi biner, misalnya 3.x.x atau 4.x.x).
*   Python 3.x.
*   `pip` (Python package installer).
*   (Untuk Windows) `winutils.exe` dan konfigurasi `HADOOP_HOME`.

## Langkah-Langkah Menjalankan Proyek (Lokal di Windows)

### 1. Konfigurasi Awal
   *   Pastikan **JDK** sudah terinstal dan environment variable `JAVA_HOME` serta `Path` sudah diatur dengan benar.
   *   Unduh dan ekstrak **Apache Kafka** (versi biner) ke direktori (misalnya, `D:\kafka\kafka_2.13-3.9.1`).
   *   (Khusus Windows) Unduh **`winutils.exe`** (misalnya, untuk Hadoop 3.x.x), buat folder `D:\Hadoop\bin`, letakkan `winutils.exe` di sana, dan atur environment variable `HADOOP_HOME` ke `D:\Hadoop` serta tambahkan `%HADOOP_HOME%\bin` ke `Path`.
   *   **Restart terminal/VS Code** setelah mengatur environment variables.

### 2. Konfigurasi Kafka & Zookeeper
   *   **Zookeeper (`config/zookeeper.properties`):**
      Ubah `dataDir` ke path absolut, misalnya:
      ```properties
      dataDir=D:/kafka/kafka_2.13-3.9.1/zookeeper-data 
      ```
      Buat folder `D:\kafka\kafka_2.13-3.9.1\zookeeper-data` secara manual.
   *   **Kafka Server (`config/server.properties`):**
      Ubah `log.dirs` ke path absolut, misalnya:
      ```properties
      log.dirs=D:/kafka/kafka_2.13-3.9.1/kafka-logs
      ```
      Buat folder `D:\kafka\kafka_2.13-3.9.1\kafka-logs` secara manual.
      Sangat disarankan untuk mengatur `advertised.listeners` juga:
      ```properties
      advertised.listeners=PLAINTEXT://localhost:9092
      ```

### 3. Jalankan Zookeeper
   Buka terminal (CMD atau PowerShell) di direktori instalasi Kafka:
   ```bash
   # Jika CMD:
   .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
   # Jika PowerShell:
   # & .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
   ```
   **Biarkan terminal ini berjalan.**

### 4. Jalankan Kafka Server
   Buka terminal **baru** di direktori instalasi Kafka:
   ```bash
   # Jika CMD:
   .\bin\windows\kafka-server-start.bat .\config\server.properties
   # Jika PowerShell:
   # & .\bin\windows\kafka-server-start.bat .\config\server.properties
   ```
   **Biarkan terminal ini berjalan.** Pastikan Zookeeper sudah berjalan sebelumnya.

### 5. Siapkan Proyek Python
   Buka terminal **baru** di direktori proyek Anda (`D:\Integrasi Sistem\DOCKERKAFKA`):
   *   **Buat dan Aktifkan Virtual Environment (jika belum):**
      ```bash
      python -m venv DockerKafka_venv 
      # Jika CMD:
      DockerKafka_venv\Scripts\activate.bat
      # Jika PowerShell:
      # . .\DockerKafka_venv\Scripts\Activate.ps1 
      # (Pastikan Execution Policy mengizinkan skrip: Set-ExecutionPolicy RemoteSigned -Scope CurrentUser -Force)
      ```
   *   **Instal Dependensi:**
      Buat file `requirements.txt` dengan isi:
      ```txt
      kafka-python
      pyspark
      ```
      Lalu jalankan:
      ```bash
      pip install -r requirements.txt
      ```

### 6. Buat Topik Kafka (Lakukan sekali)
   Buka terminal **baru** di direktori instalasi Kafka:
   ```bash
   # Jika CMD:
   .\bin\windows\kafka-topics.bat --create --topic sensor-suhu-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   .\bin\windows\kafka-topics.bat --create --topic sensor-kelembaban-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   # Jika PowerShell, gunakan & di depan perintah .bat
   ```

### 7. Jalankan Producer
   Buka dua terminal **baru** (atau gunakan kembali yang sudah ada, pastikan venv aktif di direktori proyek):
   *   **Terminal Producer Suhu:**
      ```bash
      python app/producer_suhu_gudang.py
      ```
   *   **Terminal Producer Kelembaban:**
      ```bash
      python app/producer_kelembaban_gudang.py
      ```
   **Biarkan kedua terminal producer ini berjalan.**

### 8. Jalankan Consumer PySpark
   Buka terminal **baru** (pastikan venv aktif di direktori proyek):
   ```bash
   python app/consumer_gudang_pyspark.py
   ```
   Amati output di terminal ini. Anda akan melihat log startup Spark, lalu data yang diproses dan peringatan.

## Contoh Output Consumer

### Peringatan Individual
```
[Peringatan Suhu Tinggi] Gudang G2: Suhu 85.0°C
```
```
[Peringatan Kelembaban Tinggi] Gudang G3: Kelembaban 74.0%
```

### Peringatan Gabungan dan Status Gudang
```
--- Batch Gabungan 0 ---
Hasil Pemrosesan Gudang:
[PERINGATAN KRITIS]
Gudang G1:
  - Suhu: 84.0°C
  - Kelembaban: 73.0%
  - Status: Bahaya tinggi! Barang berisiko rusak
--------------------
Gudang G2: Suhu 78.0°C, Kelembaban 68.0%, Status: Aman
--------------------
[Peringatan Suhu Tinggi] Gudang G3: Suhu 85.0°C, Status: Suhu tinggi, kelembaban normal
--------------------
```

## Dokumentasi Visual (Screenshot/GIF)

*   **Screenshot Terminal Zookeeper Berjalan:**
    ![image](https://github.com/user-attachments/assets/30947963-814f-4fb9-9c35-f415c8615465)


*   **Screenshot Terminal Kafka Server Berjalan:**
    *![image](https://github.com/user-attachments/assets/87eda15e-5014-418e-b742-13ef1c32baf4)


*   **Screenshot Terminal Producer Suhu Mengirim Data:**
    *![image](https://github.com/user-attachments/assets/71cf5a04-6d05-4a83-93de-237f1b9232bd)


*   **Screenshot Terminal Producer Kelembaban Mengirim Data:**
    ![image](https://github.com/user-attachments/assets/37db55f5-f3c5-4128-b8ce-1b56d9aae56b)


*   **Screenshot Terminal Consumer PySpark Menampilkan Output Peringatan:**
    ![image](https://github.com/user-attachments/assets/15235b1b-8285-4535-92e7-e01f7df8bdb5)
