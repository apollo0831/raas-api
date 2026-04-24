from fastapi import FastAPI
app = FastAPI()

@app.get("/")
def root():
    return {"message": "RAAS API 정상 작동!", "status": "ok"}
