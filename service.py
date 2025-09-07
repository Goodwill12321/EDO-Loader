from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
from io import BytesIO
from zipfile import ZipFile
import xmltodict
from pymongo import MongoClient

app = FastAPI()

# Настройки MongoDB (подставить свои)
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "EDO"
MONGO_COLLECTION = "schf"

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
collection = db[MONGO_COLLECTION]

class LoadXMLRequest(BaseModel):
    url: str

@app.post("/load-xml-schf")
async def load_xml(request: LoadXMLRequest):
    try:
        # Скачиваем zip-файл по URL
        resp = requests.get(request.url)
        resp.raise_for_status()

        # Открываем zip из памяти
        zip_file = ZipFile(BytesIO(resp.content))

        # Ищем первый XML-файл в архиве
        xml_file_name = next((name for name in zip_file.namelist() if name.startswith("ON_NSCHFDOPPR")), None)
        if not xml_file_name:
            raise HTTPException(status_code=400, detail="No file starting with 'ON_NSCHFDOPPR' found in the ZIP archive")

        # Читаем XML содержимое
        with zip_file.open(xml_file_name) as xml_file:
            xml_content = xml_file.read()

        # Парсим XML в dict
        xml_dict = xmltodict.parse(xml_content)

        # Вставляем в MongoDB
        collection.insert_one(xml_dict)

        return {"status": "success", "message": f"XML data inserted from {xml_file_name}"}
    except requests.RequestException as e:
        raise HTTPException(status_code=400, detail=f"HTTP error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
