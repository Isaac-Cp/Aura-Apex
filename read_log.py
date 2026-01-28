
import os

try:
    with open('bot_error.log', 'rb') as f:
        # seek to end
        f.seek(0, os.SEEK_END)
        end_pos = f.tell()
        start_pos = max(0, end_pos - 2000)
        f.seek(start_pos)
        data = f.read()
        # Decode utf-16le 
        try:
            print(data.decode('utf-16le'))
        except:
            try:
                print(data.decode('utf-8'))
            except:
                print(data)
except Exception as e:
    print(e)
