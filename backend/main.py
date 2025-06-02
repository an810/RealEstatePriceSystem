from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from visualization import router as visualization_router
from mlflow_serve import router as mlflow_serve_router
from subsribe import router as subscribe_router
from search import router as search_router

app = FastAPI(
    title="House Price Prediction API",
    description="API for predicting house prices using machine learning",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(visualization_router, tags=["visualization"])
app.include_router(mlflow_serve_router, tags=["mlflow-serve"])
app.include_router(subscribe_router, tags=["subscribe"])
app.include_router(search_router, tags=["search"])

@app.get("/")
async def root():
    return {"message": "Welcome to House Price Prediction API"} 
