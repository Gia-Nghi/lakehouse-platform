from src.ingestion.chotot.scraper import ChototIngestor

# Giả lập cấu hình
config = {"endpoint": "http://minio:9000", "access_key": "admin", "secret_key": "password123"}

# Test Chợ Tốt
ingestor = ChototIngestor(**config)
ingestor.fetch_data()
print("Đã test xong Chợ Tốt")