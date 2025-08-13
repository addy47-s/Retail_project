import chardet

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        raw_data = file.read(10000)  # Read a sample of the file (10 KB)
        result = chardet.detect(raw_data)
        return result['encoding']

file_path = "E:\\Filestomove\\shared07\\movies.dat"
encoding = detect_encoding(file_path)
print(f"Detected encoding: {encoding}")
