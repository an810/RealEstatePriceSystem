from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from visualization import router as visualization_router
from mlflow_serve import router as mlflow_serve_router
from subsribe import router as subscribe_router
from search import router as search_router

from search import load_property_data, property_cache

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[STARTUP] Preloading property data...")
    data = load_property_data()
    print(f"[STARTUP] Loaded {len(data)} records.")
    property_cache.load_and_fit(data)
    print("[STARTUP] Property cache and model loaded!")
    yield

app = FastAPI(
    title="House Price Prediction API",
    description="API for predicting house prices using machine learning",
    version="1.0.0",
    lifespan=lifespan,
)

# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(visualization_router, tags=["visualization"])
app.include_router(mlflow_serve_router, tags=["mlflow-serve"])
app.include_router(subscribe_router, tags=["subscribe"])
app.include_router(search_router, tags=["search"])

@app.get("/")
async def root():
    return {"message": "Welcome to House Price Prediction API"}
