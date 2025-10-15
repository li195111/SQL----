import re
import base64

markdown_text = open("2024大數據情境處理介紹_SQL與AI.ipynb").read()
matches = re.findall(r'!\[.*?\]\(data:image/png;base64,([A-Za-z0-9+/=]+)\)', markdown_text)

for i, img_data in enumerate(matches, start=1):
    img_bytes = base64.b64decode(img_data)
    with open(f"image_{i}.png", "wb") as f:
        f.write(img_bytes)
    print(f"✅ image_{i}.png 匯出完成")
