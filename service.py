import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
from io import BytesIO
from zipfile import ZipFile
import xmltodict
from pymongo import MongoClient
from pymongo.errors import OperationFailure


app = FastAPI()

MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost:27017")
MONGO_DB = "EDO"
MONGO_COLLECTION = "schf"


if not MONGO_USER or not MONGO_PASS:
    raise RuntimeError("MONGO_USER and MONGO_PASS environment variables must be set")

# Формируем строку подключения с авторизацией
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}/{MONGO_DB}?authSource=admin"

try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
except OperationFailure as e:
    # Тут может быть обработка при инициализации, но обычно ошибки при первой операции
    pass

class LoadXMLRequest(BaseModel):
    url: str

@app.post("/load-xml-schf")
async def load_xml(request: LoadXMLRequest):
    try:
        resp = requests.get(request.url)
        resp.raise_for_status()

        zip_file = ZipFile(BytesIO(resp.content))

        xml_file_name = next((name for name in zip_file.namelist() if name.startswith("ON_NSCHFDOPPR")), None)
        if not xml_file_name:
            raise HTTPException(status_code=400, detail="No file starting with 'ON_NSCHFDOPPR' found in the ZIP archive")

        with zip_file.open(xml_file_name) as xml_file:
            xml_content = xml_file.read()

        xml_dict = xmltodict.parse(xml_content)
        try:
            collection.insert_one(xml_dict)
        except OperationFailure as e:
            msg = f"MongoDB auth error: {str(e)}. Tried URI: {MONGO_URI}"
            raise HTTPException(status_code=500, detail=msg)

        return {"status": "success", "message": f"XML data inserted from {xml_file_name}"}
    except requests.RequestException as e:
        raise HTTPException(status_code=400, detail=f"HTTP error: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
