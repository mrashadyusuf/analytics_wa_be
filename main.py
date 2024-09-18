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
from database import engine, database
from consumer.transaction_consumer import start_consumer  
import threading

# Function to start the consumer in a separate thread
def run_consumer_in_background():
    consumer_thread = threading.Thread(target=start_consumer)
    consumer_thread.daemon = True  # This makes sure the thread exits when the main program does
    consumer_thread.start()

app = FastAPI()
@app.on_event("startup")
async def startup():
    await database.connect()
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

# Add CORS and security headers
app = add_security_headers(cors_headers(app))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
