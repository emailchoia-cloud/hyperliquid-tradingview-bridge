from api.index import app, settings

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.index:app",
        host=settings.HOST,
        port=settings.PORT,
        log_level=settings.LOG_LEVEL.lower(),
        reload=False,
    )
