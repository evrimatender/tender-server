import asyncio
import time
import os
import json
from viam.robot.client import RobotClient
from viam.components.sensor import Sensor
import firebase_admin
from firebase_admin import credentials, db
from grpclib.exceptions import StreamTerminatedError

# Firebase setup
if not firebase_admin._apps:
    firebase_key = json.loads(os.getenv("FIREBASE_KEY_JSON"))
    cred = credentials.Certificate(firebase_key)
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://evrima-tender-tracking-default-rtdb.firebaseio.com/'
    })

# Connect to VIAM
async def connect():
    while True:
        try:
            opts = RobotClient.Options.with_api_key(
                api_key='g7wj2rvi8jzujdacjw4kifr4nh2e3qs1',
                api_key_id='42e9d6a7-549d-4d88-8897-6b088eaeadc5'
            )
            machine = await RobotClient.at_address('njordlinkplus.u1ho16k8rd.viam.cloud', opts)
            print("✅ Connected to VIAM Cloud")
            return machine
        except Exception as e:
            print(f"❌ Connection failed: {e}. Retrying in 10s...")
            await asyncio.sleep(10)

# Get previous value from Firebase
def get_previous_value(ref, field):
    current = ref.get()
    if current and field in current:
        return current[field]
    return 0

# Main logic
async def main_loop(machine):
    all_pgn = Sensor.from_robot(machine, "all-pgn")

    while True:
        try:
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
                db.reference("positions/ship/latest").set({
                    "timestamp": timestamp,
                    "lat": ships[0]["Latitude"],
                    "lon": ships[0]["Longitude"],
                    "heading": ships[0].get("COG") or get_previous_value(ref, "heading"),
                    "speed": ships[0].get("SOG") or get_previous_value(ref, "speed"),
                })
                print("✅ Ship position updated")

            if tenders3:
                ref = db.reference("positions/tender3/latest")
                db.reference("positions/tender3/latest").set({
                    "timestamp": timestamp,
                    "lat": tenders3[0]["Latitude"],
                    "lon": tenders3[0]["Longitude"],
                    "heading": tenders3[0].get("COG") or get_previous_value(ref, "heading"),
                    "speed": tenders3[0].get("SOG") or get_previous_value(ref, "speed"),
                })
                print("✅ Tender 3 position updated")

            if tenders4:
                ref = db.reference("positions/tender4/latest")
                db.reference("positions/tender4/latest").set({
                    "timestamp": timestamp,
                    "lat": tenders4[0]["Latitude"],
                    "lon": tenders4[0]["Longitude"],
                    "heading": tenders4[0].get("COG") or get_previous_value(ref, "heading"),
                    "speed": tenders4[0].get("SOG") or get_previous_value(ref, "speed"),
                })
                print("✅ Tender 4 position updated")

            print("⏳ Waiting for next update...\n")
            await asyncio.sleep(2)

        except StreamTerminatedError as e:
            print(f"⚠️ Sensor stream terminated: {e}. Reconnecting...")
            raise  # Triggers reconnect in run_loop

        except Exception as e:
            print(f"⚠️ Unexpected error in main loop: {e}")
            await asyncio.sleep(5)  # Continue after short delay

# Restart mechanism
async def run_loop():
    while True:
        machine = await connect()
        try:
            await main_loop(machine)
        except Exception as e:
            print(f"🔁 Restarting main loop due to error: {e}")
        finally:
            try:
                await machine.close()
                print("🔌 VIAM connection closed")
            except Exception:
                pass
            await asyncio.sleep(5)

# Entrypoint
if __name__ == "__main__":
    asyncio.run(run_loop())
