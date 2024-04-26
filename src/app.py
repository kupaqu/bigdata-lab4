from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from src.logger import Logger

import numpy as np
import pandas as pd
import joblib
import os
import json
import uuid

from hbase.rest_client import HBaseRESTClient
from hbase.admin import HBaseAdmin
from hbase.put import Put
from hbase.get import Get

SHOW_LOG = True

# logger
logger = Logger(SHOW_LOG)
log = logger.get_logger(__name__)

# hbase connection
client = HBaseRESTClient(['http://localhost:8081'])
admin = HBaseAdmin(client)
put = Put(client)
get = Get(client)
log.info('Connected to HBase.')

# table check
tables = json.loads(admin.tables())
admin.table_create_or_update("request", [{"name":"r"}])
log.info('Table "request" created.')

# Создаем экземпляр FastAPI
app = FastAPI()

# Загружаем модель
project_path = os.getcwd()
models_path = os.path.join(project_path, 'models')
model_path = os.path.join(models_path, "lr.sav")
model = joblib.load(model_path)

# Определяем класс Pydantic модели для входных данных
class InputData(BaseModel):
    X: list
    # y: list

# Определяем эндпоинт для предсказаний
@app.post("/predict/")
async def predict(input_data: InputData):
    try:
        # Преобразуем данные в DataFrame
        df = pd.DataFrame(input_data.X, columns=[str(i) for i in range(len(input_data.X[0]))])

        # Выполняем предсказание
        predictions = model.predict(df).tolist()

        # Формируем ответ
        response = {"predictions": predictions}

        # логирование в hbase
        row_key = str(uuid.uuid4())
        put.put(
            tbl_name="request",
            row_key=str(uuid.uuid4()),
            cell_map={
                "r:X": df.to_string(),
                "r:pred": str(predictions[0])
            }
        )
        
        # log.info(get.get(tbl_name="request", row_key=row_key))

        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.openapi.utils import get_openapi
from fastapi.openapi.docs import get_swagger_ui_html

@app.get("/docs", response_class=HTMLResponse)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="API docs")

@app.get("/openapi.json", include_in_schema=False)
async def get_open_api_endpoint():
    return JSONResponse(get_openapi(title="Your Project Name", version="0.1.0", routes=app.routes))
