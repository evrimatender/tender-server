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


async def connect():
    opts = RobotClient.Options.with_api_key(
        api_key='g7wj2rvi8jzujdacjw4kifr4nh2e3qs1',
        api_key_id='42e9d6a7-549d-4d88-8897-6b088eaeadc5'
    )
    return await RobotClient.at_address('njordlinkplus.u1ho16k8rd.viam.cloud', opts)

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
        db.reference("positions/ship/latest").set({
            "timestamp": timestamp,
            "lat": ships[0]["Latitude"],
            "lon": ships[0]["Longitude"],
            "heading": ships[0].get("COG"),
            "speed": ships[0].get("SOG"),
        })
        print("✅ Ship position updated")

    if tenders3:
        db.reference("positions/tender3/latest").set({
            "timestamp": timestamp,
            "lat": tenders3[0]["Latitude"],
            "lon": tenders3[0]["Longitude"],
            "heading": tenders3[0].get("COG"),
            "speed": tenders3[0].get("SOG"),
        })
        print("✅ Tender 3 position updated")

    if tenders4:
        db.reference("positions/tender4/latest").set({
            "timestamp": timestamp,
            "lat": tenders4[0]["Latitude"],
            "lon": tenders4[0]["Longitude"],
            "heading": tenders4[0].get("COG"),
            "speed": tenders4[0].get("SOG"),
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
