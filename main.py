from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from decimal import Decimal
import os
from dotenv import load_dotenv
from .database import get_db_connection
from .models import SchedulePush

# Load environment variables
load_dotenv()

app = FastAPI()

# Configuration
API_TOKEN = os.getenv("API_TOKEN")
MAX_MW_LIMIT = float(os.getenv("MAX_MW_LIMIT", 1.0))

# Security Dependency
def verify_token(x_api_token: str = Header(...)):
    if x_api_token != API_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid API Token")
    
##keinen string zurückgeben
#JSONResponse change to some class
#

# Refactored logic to handle both production and simulation tables
def process_schedule_push(data: SchedulePush, table_name: str):
    # 1. Validate Duplicate Quarters (New Check)
    seen_quarters = set()
    for q in data.Quarters:
        if q.Quarter in seen_quarters:
            raise HTTPException(
                status_code=400, 
                detail=f"Duplicate detected: Quarter {q.Quarter} appears multiple times."
            )
        seen_quarters.add(q.Quarter)

    # 2. Validate Max MW
    for q in data.Quarters:
        if abs(q.Quantity) > MAX_MW_LIMIT:
            raise HTTPException(
                status_code=400, 
                detail=f"Quantity {q.Quantity} exceeds limit of {MAX_MW_LIMIT} MW"
            )

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    cursor = conn.cursor()
    
    try:
        # 3. Version Check (New Check)
        # We explicitly look up the current version before inserting
        # Using f-string for table_name is safe here as it comes from trusted code arguments
        check_query = f"SELECT MAX(version) FROM {table_name} WHERE datum = %s AND bilanzkreis = %s"
        cursor.execute(check_query, (data.Datum, data.Bilanzkreis))
        result = cursor.fetchone()
        
        # If result is None (no data yet), we treat current version as -1
        current_version = result[0] if result and result[0] is not None else -1
        
        if data.Version <= current_version:
             raise HTTPException(
                 status_code=409, # 409 Conflict is standard for version/state conflicts
                 detail=f"Rejected: Incoming version {data.Version} is not higher than existing version {current_version}"
             )

        # 4. Insert Data
        insert_query = f"""
            INSERT INTO {table_name} (datum, bilanzkreis, quarter, quantity, version)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                quantity = VALUES(quantity),
                version = VALUES(version)
        """
        
        batch_values = [
            (data.Datum, data.Bilanzkreis, q.Quarter, q.Quantity, data.Version)
            for q in data.Quarters
        ]
        
        cursor.executemany(insert_query, batch_values)
        conn.commit()
        
        return {"status": "success", "message": f"Processed {len(batch_values)} quarters", "environment": table_name}

    except HTTPException as he:
        # Re-raise HTTP exceptions (like the version conflict)
        conn.rollback()
        raise he
    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

#####################################PUSH######################################    
@app.post("/push")
async def push_schedule(data: SchedulePush, x_api_token: str = Header(...)):
    verify_token(x_api_token)
    return process_schedule_push(data, "ok_energy_schedule")

@app.post("/simulation/push")
async def push_simulation_schedule(data: SchedulePush, x_api_token: str = Header(...)):
    verify_token(x_api_token)
    return process_schedule_push(data, "ok_energy_schedule_simulation")
        
#####################################PULL######################################
@app.get("/pull")
async def pull_trades(
    start_date: str = Query(..., description="Format YYYYMMDD (e.g. 20251026)"), 
    end_date: str = Query(..., description="Format YYYYMMDD (e.g. 20251129)"),
    x_api_token: str = Header(...)
):
    verify_token(x_api_token)
    
    if len(start_date) != 8 or len(end_date) != 8:
        raise HTTPException(status_code=400, detail="Dates must be in YYYYMMDD format")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    cursor = conn.cursor(dictionary=True)
    #######################
    try:
        query = """
            SELECT `B/S`, `LCtrct`, `Qty`, `Prc`, `Text`, `TradeID`, `Area`
            FROM trades
            WHERE 
                `Text` LIKE '%OKENERGY%'
                AND LEFT(`LCtrct`, 8) >= %s 
                AND LEFT(`LCtrct`, 8) <= %s
        """
        
        cursor.execute(query, (start_date, end_date))
        trades = cursor.fetchall() #######################
        
        # --- NEW: Convert Decimal to float for JSON ---
        for trade in trades:
            for key, value in trade.items():
                if isinstance(value, Decimal):
                    trade[key] = float(value)
        # ---------------------------------------------
        
        return JSONResponse(content={"trades": trades})

    except Exception as e:
        # Print error to logs so we can see it next time
        print(f"ERROR: {e}") 
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()