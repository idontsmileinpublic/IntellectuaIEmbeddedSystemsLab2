import asyncio
import json
import psycopg2
from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from datetime import datetime
from pydantic import BaseModel, field_validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()
# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)


# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )

class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}

# FastAPI CRUDL endpoints

# Створення даних
@app.post("/processed_agent_data/", response_model=ProcessedAgentDataInDB)
async def create_processed_agent_data(data: ProcessedAgentData):
    async with SessionLocal() as session:
        # Створюємо новий запис в базі даних
        db_data = ProcessedAgentDataInDB(**data.dict(), timestamp=datetime.utcnow())
        session.add(db_data)
        session.commit()
        # Надсилаємо дані підписникам WebSocket
        if data.agent_data.user_id in subscriptions:
            message = json.dumps(db_data.dict())
            for websocket in subscriptions[data.agent_data.user_id]:
                await websocket.send_text(message)
        return db_data


# Читання даних за ідентифікатором
@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(processed_agent_data_id: int):
    with SessionLocal() as session:
        # Зчитуємо дані з бази даних за ідентифікатором
        data = session.query(ProcessedAgentDataInDB).filter(ProcessedAgentDataInDB.id == processed_agent_data_id).first()
        if not data:
            raise HTTPException(status_code=404, detail="Item not found")
        return data


# Отримання списку всіх даних
@app.get("/processed_agent_data/", response_model=List[ProcessedAgentDataInDB])
def list_processed_agent_data():
    with SessionLocal() as session:
        # Зчитуємо всі дані з бази даних
        return session.query(ProcessedAgentDataInDB).all()


# Оновлення даних за ідентифікатором
@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    with SessionLocal() as session:
        # Оновлюємо дані в базі даних за ідентифікатором
        db_data = session.query(ProcessedAgentDataInDB).filter(ProcessedAgentDataInDB.id == processed_agent_data_id).first()
        if not db_data:
            raise HTTPException(status_code=404, detail="Item not found")
        for field, value in data.dict().items():
            setattr(db_data, field, value)
        session.commit()
        return db_data


# Видалення даних за ідентифікатором
@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def delete_processed_agent_data(processed_agent_data_id: int):
    with SessionLocal() as session:
        # Видаляємо дані з бази даних за ідентифікатором
        data = session.query(ProcessedAgentDataInDB).filter(ProcessedAgentDataInDB.id == processed_agent_data_id).first()
        if not data:
            raise HTTPException(status_code=404, detail="Item not found")
        session.delete(data)
        session.commit()
        return data

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)