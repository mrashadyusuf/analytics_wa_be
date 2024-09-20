from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, SecurityScopes
from fastapi.openapi.models import OAuthFlows as OAuthFlowsModel, OAuthFlowPassword
from fastapi.openapi.models import SecurityScheme
from fastapi.openapi.utils import get_openapi
from auth import get_current_user, User
from routes.auth import router as auth_router
from routes.transaction import router as transaction_router
from routes.test import router as test_router
from routes.etl import etl
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
from utilities.instagram import schedule_get_follower

from routes.etl import data_transaksi, chat_wa, sum_transaksi, sum_average_sales, sum_customer, sum_model, sum_region, sum_sales_trend, sum_sales_trend_pertanggal, sum_store, sum_top_produk, sum_wa, sum_customer_follower

from database import engine, database
from consumer.transaction_consumer import start_consumer  
import threading

# Function to start the consumer in a separate thread
def run_consumer_in_background():
    consumer_thread = threading.Thread(target=start_consumer)
    consumer_thread.daemon = True  # This makes sure the thread exits when the main program does
    consumer_thread.start()

app = FastAPI()
@app.get("/")
async def root():
    return {"message": "TLE API version 1.0"}

scheduler = True
def run_data_transaksi():
    asyncio.run(data_transaksi(scheduler))

def run_chat_wa():
    asyncio.run(chat_wa(scheduler))

def run_data_summary():
    asyncio.run(sum_transaksi())
    asyncio.run(sum_average_sales())
    asyncio.run(sum_customer())
    asyncio.run(sum_model())
    asyncio.run(sum_region())
    asyncio.run(sum_sales_trend())
    asyncio.run(sum_sales_trend_pertanggal())
    asyncio.run(sum_store())
    asyncio.run(sum_top_produk())
    asyncio.run(sum_wa())
    asyncio.run(sum_customer_follower())

def run_get_follower():
    asyncio.run(schedule_get_follower())

@app.on_event("startup")
async def startup():
    await database.connect()
    
    # jalanin scheduler    
    # sched = BackgroundScheduler()
    # sched.add_job(run_data_transaksi, trigger='interval', minutes=5)
    # sched.add_job(run_chat_wa, trigger='interval', seconds=60)
    # sched.add_job(run_data_summary, trigger='interval', minutes=10)
    # sched.add_job(run_get_follower, trigger='interval', minutes=60)
    # sched.start()

    run_consumer_in_background()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

# Define the security scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Your API",
        version="1.0.0",
        description="This is your API with JWT Authorization",
        routes=app.routes,
    )
    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
        }
    }
    for path in openapi_schema["paths"].values():
        for operation in path.values():
            operation["security"] = [{"BearerAuth": []}]
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# CORS middleware setup
def cors_headers(app):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )
    return app

# Security headers middleware
def add_security_headers(app):
    @app.middleware("http")
    async def add_headers(request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response
    
    return app



# Include the routers from the routes module
app.include_router(auth_router, prefix="/auth")
app.include_router(transaction_router, prefix="/transactions")
app.include_router(test_router, prefix="/test")
app.include_router(etl, prefix="/etl")

# Add CORS and security headers
app = add_security_headers(cors_headers(app))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
