import asyncio
import time
import os
import json
import logging
from viam.robot.client import RobotClient
from viam.components.sensor import Sensor
import firebase_admin
from firebase_admin import credentials, db
from grpclib.exceptions import StreamTerminatedError
from asyncio import CancelledError, TimeoutError

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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
            logging.info("✅ Connected to VIAM Cloud")
            return machine
        except Exception as e:
            logging.warning(f"❌ Connection failed: {e}. Retrying in 10s...")
            await asyncio.sleep(10)

# Get previous value from Firebase
def get_previous_value(ref, field):
    current = ref.get()
    return current.get(field) if current and field in current else 0

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
                if user_id == 215001000:
                    ships.append(payload)
                elif user_id == 982150011:
                    tenders1.append(payload) 
                elif user_id == 982150012:
                    tenders2.append(payload)    
                elif user_id == 982150013:
                    tenders3.append(payload)
                elif user_id == 982150014:
                    tenders4.append(payload)

            timestamp = int(time.time() * 1000)

            if ships:
                ref = db.reference("positions/ship/latest")
                ref.set({
                    "timestamp": timestamp,
                    "lat": ships[0]["Latitude"],
                    "lon": ships[0]["Longitude"],
                    "heading": ships[0].get("COG") or get_previous_value(ref, "heading"),
                    "speed": ships[0].get("SOG") or get_previous_value(ref, "speed"),
                })
                logging.info("✅ Ship position updated")
            if tenders1:
                ref = db.reference("positions/tender3/latest")
                ref.set({
                    "timestamp": timestamp,
                    "lat": tenders1[0]["Latitude"],
                    "lon": tenders1[0]["Longitude"],
                    "heading": tenders1[0].get("COG") or get_previous_value(ref, "heading"),
                    "speed": tenders1[0].get("SOG") or get_previous_value(ref, "speed"),
                })
                logging.info("✅ Tender 1 position updated")
            if tenders2:
                ref = db.reference("positions/tender3/latest")
                ref.set({
                    "timestamp": timestamp,
                    "lat": tenders2[0]["Latitude"],
                    "lon": tenders2[0]["Longitude"],
                    "heading": tenders2[0].get("COG") or get_previous_value(ref, "heading"),
                    "speed": tenders2[0].get("SOG") or get_previous_value(ref, "speed"),
                })
                logging.info("✅ Tender 2 position updated")
            if tenders3:
                ref = db.reference("positions/tender3/latest")
                ref.set({
                    "timestamp": timestamp,
                    "lat": tenders3[0]["Latitude"],
                    "lon": tenders3[0]["Longitude"],
                    "heading": tenders3[0].get("COG") or get_previous_value(ref, "heading"),
                    "speed": tenders3[0].get("SOG") or get_previous_value(ref, "speed"),
                })
                logging.info("✅ Tender 3 position updated")

            if tenders4:
                ref = db.reference("positions/tender4/latest")
                ref.set({
                    "timestamp": timestamp,
                    "lat": tenders4[0]["Latitude"],
                    "lon": tenders4[0]["Longitude"],
                    "heading": tenders4[0].get("COG") or get_previous_value(ref, "heading"),
                    "speed": tenders4[0].get("SOG") or get_previous_value(ref, "speed"),
                })
                logging.info("✅ Tender 4 position updated")

            await asyncio.sleep(2)

        except (StreamTerminatedError, TimeoutError, CancelledError) as e:
            logging.warning(f"⚠️ Connection dropped: {type(e).__name__}. Reconnecting...")
            raise e  # Triggers reconnect in run_loop

        except Exception as e:
            logging.error(f"⚠️ Unexpected error in main loop: {e}")
            await asyncio.sleep(5)  # Allow retry without crashing

# Restart mechanism
async def run_loop():
    while True:
        machine = await connect()
        try:
            await main_loop(machine)
        except Exception as e:
            logging.warning(f"🔁 Restarting main loop due to: {e}")
        finally:
            try:
                await machine.close()
                logging.info("🔌 VIAM connection closed")
            except Exception:
                pass
            await asyncio.sleep(5)

# Entrypoint
if __name__ == "__main__":
    asyncio.run(run_loop())
