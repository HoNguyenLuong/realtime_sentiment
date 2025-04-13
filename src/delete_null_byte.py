import os

def clean_file(path):
    with open(path, 'rb') as f:
        data = f.read()
    if b'\x00' in data:
        new_data = data.replace(b'\x00', b'')
        with open(path, 'wb') as f:
            f.write(new_data)
        print(f"Cleaned null bytes in: {path}")

for root, _, files in os.walk(''):
    for fn in files:
        if fn == '__init__.py':
            clean_file(os.path.join(root, fn))