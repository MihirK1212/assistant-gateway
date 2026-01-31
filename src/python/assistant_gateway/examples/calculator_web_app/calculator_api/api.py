from fastapi import FastAPI, HTTPException

app = FastAPI(title="Calculator API")


@app.get("/add")
def add(a: float, b: float) -> dict:
    return {"result": a + b}


@app.get("/multiply")
def multiply(a: float, b: float) -> dict:
    return {"result": a * b}


@app.get("/divide")
def divide(a: float, b: float) -> dict:
    if b == 0:
        raise HTTPException(status_code=400, detail="Division by zero")
    return {"result": a / b}


@app.get("/mihir_custom_transform")
def mihir_custom_transform(a: float) -> dict:
    return {"result": a * 28 + 12}


@app.get("/mihir_custom_series")
def mihir_custom_series(a: float) -> list[float]:
    return [a - 2, a - 1, a, a + 1, a + 2]


@app.post("/mihir_custom_log")
def mihir_custom_log(message: str) -> dict:
    import os
    import time
    import random
    from datetime import datetime, timezone, timedelta

    received_timestamp = datetime.now(
        timezone(timedelta(hours=5, minutes=30))
    ).strftime("%Y-%m-%d %H:%M:%S")

    # Sleep for a random time between 60 and 80 seconds
    sleep_time = random.uniform(60, 80)
    time.sleep(sleep_time)

    log_file = os.path.join(os.path.dirname(__file__), "logs.txt")

    printed_timestamp = datetime.now(
        timezone(timedelta(hours=5, minutes=30))
    ).strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[Received at: ({received_timestamp})] [Printed at: ({printed_timestamp})] [Waited for: {sleep_time} seconds] {{{message}}}\n"

    with open(log_file, "a") as f:
        f.write(log_entry)
    return {"status": "logged", "message": message}
