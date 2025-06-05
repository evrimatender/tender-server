import asyncio
import time
from viam.robot.client import RobotClient
from viam.components.sensor import Sensor
import os
import json
import firebase_admin
from firebase_admin import credentials, db

if not firebase_admin._apps:
    firebase_key = json.loads(os.getenv("FIREBASE_KEY_JSON"))
    cred = credentials.Certificate(firebase_key)
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://evrima-tender-tracking-default-rtdb.firebaseio.com/'
    })


def get_previous_value(ref, field):
    current = ref.get()
    if current and field in current:
        return current[field]
    return 0  # or another sensible default

async def main(machine):
    print("Querying VIAM sensor...")
    all_pgn = Sensor.from_robot(machine, "all-pgn")
    all_pgn_return_value = await all_pgn.get_readings()

    ships, tenders3, tenders4 = [], [], []

    for key, payload in all_pgn_return_value.items():
        if not isinstance(payload, dict):
            continue
        user_id = int(payload.get("User ID", -1))
        if user_id == 215001000 and payload.get("Latitude") and payload.get("Longitude"):
            ships.append(payload)
            print(f"Ship payload: {payload}")
        elif user_id == 982150013 and payload.get("Latitude") and payload.get("Longitude"):
            tenders3.append(payload)
        elif user_id == 982150014 and payload.get("Latitude") and payload.get("Longitude"):
            tenders4.append(payload)

    timestamp = int(time.time() * 1000)

    if ships:
        ref = db.reference("positions/ship/latest")
        prev_heading = get_previous_value(ref, "heading")
        prev_speed = get_previous_value(ref, "speed")
        db.reference("positions/ship/latest").set({
            "timestamp": timestamp,
            "lat": ships[0]["Latitude"],
            "lon": ships[0]["Longitude"],
            "heading": ships[0].get("COG") if ships[0].get("COG") is not None else prev_heading,
            "speed": ships[0].get("SOG") if ships[0].get("SOG") is not None else prev_speed,
        })
        print("✅ Ship position updated")

    if tenders3:
        ref = db.reference("positions/tender3/latest")
        prev_heading = get_previous_value(ref, "heading")
        prev_speed = get_previous_value(ref, "speed")
        db.reference("positions/tender3/latest").set({
            "timestamp": timestamp,
            "lat": tenders3[0]["Latitude"],
            "lon": tenders3[0]["Longitude"],
            "heading": tenders3[0].get("COG") if tenders3[0].get("COG") is not None else prev_heading,
            "speed": tenders3[0].get("SOG") if tenders3[0].get("SOG") is not None else prev_speed,
        })
        print("✅ Tender 3 position updated")

    if tenders4:
        ref = db.reference("positions/tender4/latest")
        prev_heading = get_previous_value(ref, "heading")
        prev_speed = get_previous_value(ref, "speed")
        db.reference("positions/tender4/latest").set({
            "timestamp": timestamp,
            "lat": tenders4[0]["Latitude"],
            "lon": tenders4[0]["Longitude"],
            "heading": tenders4[0].get("COG") if tenders4[0].get("COG") is not None else prev_heading,
            "speed": tenders4[0].get("SOG") if tenders4[0].get("SOG") is not None else prev_speed,
        })
        print("✅ Tender 4 position updated")

    print("Waiting for next update...\n")

async def run_loop():
    print("Connecting to VIAM Cloud...")
    machine = await connect()
    print("Connected to VIAM Cloud!")
    try:
        while True:
            await main(machine)
            await asyncio.sleep(2)
    finally:
        print("Closing VIAM connection...")
        await machine.close()

if __name__ == "__main__":
    asyncio.run(run_loop())

