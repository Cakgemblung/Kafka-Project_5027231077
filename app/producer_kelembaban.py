import time
import json
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

KAFKA_TOPIC = "sensor-kelembaban-gudang"
KAFKA_BROKER = 'localhost:9092'
GUDANG_IDS = ["G1", "G2", "G3"] # Tambahkan G4 jika Anda ingin menguji kasus gudang hanya dengan kelembaban tinggi

def create_producer_instance():
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            acks='all'
        )
        print(f"Producer Kelembaban Gudang berhasil terhubung ke Kafka broker di {KAFKA_BROKER}")
        return producer
    except NoBrokersAvailable:
        print(f"ERROR (Kelembaban Gudang): Tidak ada broker Kafka yang tersedia di {KAFKA_BROKER}.")
    except KafkaError as e:
        print(f"ERROR (Kelembaban Gudang): Gagal terhubung ke Kafka: {e}")
    except Exception as e:
        print(f"ERROR (Kelembaban Gudang): Kesalahan tak terduga saat membuat producer: {e}")
    return None

def send_kelembaban_data(producer):
    if not producer:
        print("Producer Kelembaban Gudang tidak terinisialisasi. Gagal mengirim data.")
        return

    print("Mulai mengirim data kelembaban gudang... Tekan Ctrl+C untuk berhenti.")
    try:
        while True:
            gudang_id = random.choice(GUDANG_IDS)
            # Kelembaban bisa bervariasi, beberapa di atas 70, beberapa di bawah
            kelembaban = round(random.uniform(60.0, 80.0), 1) 

            data_kelembaban = {
                'gudang_id': gudang_id,
                'kelembaban': kelembaban,
                'timestamp': time.time() # Menggunakan Unix timestamp
            }
            
            producer.send(KAFKA_TOPIC, value=data_kelembaban)
            print(f"Mengirim data kelembaban: {data_kelembaban}")
            
            time.sleep(1)  # Kirim data setiap detik
    except KeyboardInterrupt:
        print("\nProducer Kelembaban Gudang dihentikan oleh pengguna.")
    except Exception as e:
        print(f"ERROR (Kelembaban Gudang): Terjadi kesalahan saat mengirim data: {e}")
    finally:
        if producer:
            print("Menutup producer Kelembaban Gudang...")
            producer.flush()
            producer.close()
            print("Producer Kelembaban Gudang ditutup.")

if __name__ == "__main__":
    kelembaban_producer = create_producer_instance()
    if kelembaban_producer:
        send_kelembaban_data(kelembaban_producer)