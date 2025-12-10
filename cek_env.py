import os
from dotenv import load_dotenv

# 1. Cek lokasi sekarang
print(f'?? Lokasi script berjalan: {os.getcwd()}')

# 2. Cek apakah file .env ada secara fisik
file_ada = os.path.exists('.env')
print(f'?? Apakah file .env terdeteksi? {file_ada}')

# 3. Coba paksa baca
load_dotenv(verbose=True)

# 4. Cek isinya
key = os.getenv('NEWS_API_KEY')
if key:
    print(f'? SUKSES! API Key terbaca: {key[:5]}********')
else:
    print('? GAGAL! Variable NEWS_API_KEY masih kosong/None.')
