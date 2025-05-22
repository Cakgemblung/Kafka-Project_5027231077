import time
import json
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

KAFKA_TOPIC = "sensor-suhu-gudang"
KAFKA_BROKER = 'localhost:9092'
GUDANG_IDS = ["G1", "G2", "G3"]

def create_producer_instance():
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            acks='all'
        )
        print(f"Producer Suhu Gudang berhasil terhubung ke Kafka broker di {KAFKA_BROKER}")
        return producer
    except NoBrokersAvailable:
        print(f"ERROR (Suhu Gudang): Tidak ada broker Kafka yang tersedia di {KAFKA_BROKER}.")
    except KafkaError as e:
        print(f"ERROR (Suhu Gudang): Gagal terhubung ke Kafka: {e}")
    except Exception as e:
        print(f"ERROR (Suhu Gudang): Kesalahan tak terduga saat membuat producer: {e}")
    return None

def send_suhu_data(producer):
    if not producer:
        print("Producer Suhu Gudang tidak terinisialisasi. Gagal mengirim data.")
        return

    print("Mulai mengirim data suhu gudang... Tekan Ctrl+C untuk berhenti.")
    try:
        while True:
            gudang_id = random.choice(GUDANG_IDS)
            # Suhu bisa bervariasi, beberapa di atas 80, beberapa di bawah
            suhu = round(random.uniform(70.0, 90.0), 1) 
            
            data_suhu = {
                'gudang_id': gudang_id,
                'suhu': suhu,
                'timestamp': time.time() # Menggunakan Unix timestamp untuk kemudahan join window
            }
            
            producer.send(KAFKA_TOPIC, value=data_suhu)
            print(f"Mengirim data suhu: {data_suhu}")
            
            time.sleep(1)  # Kirim data setiap detik
    except KeyboardInterrupt:
        print("\nProducer Suhu Gudang dihentikan oleh pengguna.")
    except Exception as e:
        print(f"ERROR (Suhu Gudang): Terjadi kesalahan saat mengirim data: {e}")
    finally:
        if producer:
            print("Menutup producer Suhu Gudang...")
            producer.flush()
            producer.close()
            print("Producer Suhu Gudang ditutup.")

if __name__ == "__main__":
    suhu_producer = create_producer_instance()
    if suhu_producer:
        send_suhu_data(suhu_producer)