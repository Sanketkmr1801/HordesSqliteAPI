from peewee import Model, PostgresqlDatabase, IntegerField, CharField, DateTimeField
from peewee import fn, JOIN
from fastapi import FastAPI
import httpx
import os
import json
import sys
import asyncio
import traceback
import datetime
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

# Fetch environment variables
postgres_url = os.getenv("POSTGRES_URL")
postgres_prisma_url = os.getenv("POSTGRES_PRISMA_URL")
postgres_url_non_pooling = os.getenv("POSTGRES_URL_NON_POOLING")
postgres_user = os.getenv("POSTGRES_USER")
postgres_host = os.getenv("POSTGRES_HOST")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_database = os.getenv("POSTGRES_DATABASE")

# Create the PostgresqlDatabase instance
db = PostgresqlDatabase(
    database=postgres_database,
    user=postgres_user,
    password=postgres_password,
    host=postgres_host,
    port=5432  # replace with your actual port if it's different
)
# import http.server
# import socketserver

# PORT = 8080

# Handler = http.server.SimpleHTTPRequestHandler

# with socketserver.TCPServer(("", PORT), Handler) as httpd:
#     print("serving at port", PORT)
#     httpd.serve_forever()

class PlayerLog(Model):
  id = CharField(primary_key = True)
  dps = IntegerField()
  dpsid = IntegerField()
  hps = IntegerField()
  hpsid = IntegerField()
  mps = IntegerField()
  mpsid = IntegerField()
  dps60 = IntegerField()
  dpsid60 = IntegerField()
  hps60 = IntegerField()
  hpsid60 = IntegerField()
  mps60 = IntegerField()
  mpsid60 = IntegerField()
  deaths = IntegerField()
  kills = IntegerField()
  name = CharField()
  faction = IntegerField()
  class_ = IntegerField()

  class Meta:
    database = db  # This model uses the "hordes.db" database.
    indexes = (
        (('name', ), False),  # Creating an index on the 'name' column
    )
    table_name = 'playerlog'


class BossLog(Model):
  killid = IntegerField(primary_key=True)
  duration = IntegerField()
  time = DateTimeField()

  class Meta:
    database = db
    table_name = 'bosslog'
class_code = {'warrior': 0, 'mage': 1, 'archer': 2, 'shaman': 3}
faction_code = {"vg": 0, "bl": 1}


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Connecting to the database...")

    try:
  # Connect to the database and create tables
      db.connect()
      db.create_tables([PlayerLog], safe=True)  # Create tables if they don't exist
      db.create_tables([BossLog], safe=True)  # Create tables if they don't exist
      
    except OperationalError as e:
      # Handle the exception
      print(f"Error creating tables: {e}")
        # Additional setup or operations if needed

    # Schedule update_db every 2 hours
    asyncio.create_task(schedule_update_db())

    yield

    # Disconnect from the database when the FastAPI app shuts down
    print("Disconnecting from the database...")
    db.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"message": "hehe hi there <3"}


@app.get("/update")
async def update():
    await update_db()
    return {"message": "manually updating db..."}


@app.get("/rankings/{player_name}/{required_arg}/{optional_args}")
async def ranking(player_name: str, required_arg: str, optional_args: str):
    if (player_name == ""): return {"message": "no player name given"}
    required_arg = required_arg.lower()
    optional_params = {
        "vg": "faction",
        "bl": "faction",
        "month": "time",
        "warrior": "class",
        "mage": "class",
        "shaman": "class",
        "archer": "class",
        "true": "duration"
    }

    options = optional_args.split(" ")

    query_params = {
        "faction": "all",
        "time": "all",
        "class": "all",
        "duration": "false"
    }

    for option in options:
        option = option.lower()
        if (option not in optional_params):
            pass
        else:
            query_params[optional_params[option]] = option

    query = ""
    # Check if the required argument is a valid stat parameter
    valid_stat_params = ["dps", "mps", "hps", "kills", "deaths"]
    if required_arg in valid_stat_params:

        # Define the fields to be selected dynamically based on required_arg
        selected_fields = [
            PlayerLog.kills,
            PlayerLog.deaths,
        ]
        if required_arg not in ["deaths", "kills"]:
            if(query_params["duration"] == "true"):
                selected_fields.append(
                    getattr(PlayerLog, required_arg + "id60").alias("killid"))
                selected_fields.append(
                    getattr(PlayerLog, required_arg + "60").alias('max')
                )
            else:
                selected_fields.append(
                    getattr(PlayerLog, required_arg + "id").alias("killid")
                    )
                selected_fields.append(
                    getattr(PlayerLog, required_arg).alias('max')
                )
        # Add the dynamic fields to the SELECT clause
        if required_arg not in ["deaths", "kills"]:
            query = (PlayerLog.select(
                PlayerLog.name, *selected_fields, BossLog.time.alias("time"), 
                BossLog.duration.alias("duration"), 
                PlayerLog.faction, PlayerLog.class_)
                )

            if query_params["duration"] == "true":
                query = query.join(BossLog, on=(getattr(PlayerLog, required_arg + "id60") == BossLog.killid))
                query = query.order_by(getattr(PlayerLog, required_arg + "60").desc())
            else:
                query = query.join(BossLog, on=(getattr(PlayerLog, required_arg + "id") == BossLog.killid))
                query = query.order_by(getattr(PlayerLog, required_arg).desc())
        else:
            query = (PlayerLog.select(
                PlayerLog.name, *selected_fields,
                PlayerLog.faction, PlayerLog.class_)
                ).order_by(getattr(PlayerLog, required_arg).desc())

    # Apply optional filters
    if query_params["faction"] != "all":
        faction = faction_code[query_params["faction"]]
        query = query.where(PlayerLog.faction == faction)

    if query_params["class"] != "all":
        class_ = class_code[query_params["class"]]
        query = query.where(PlayerLog.class_ == class_)

    results = query.execute()
    result_objects = []

    limit = 10  # Set your desired limit
    print(query)
    foundMyself = False
    for i, row in enumerate(results):

        record_dict = {
            "index": i + 1,
            "name": row.name,
            "faction": row.faction,
            "class": row.class_,
        }
        if (required_arg != "deaths" and required_arg != "kills"):
            duration = format_seconds(row.bosslog.duration)
            time = formatTime(row.bosslog.time)
            record_dict["time"] = time
            record_dict["duration"] = duration
            record_dict["killid"] = row.killid
            record_dict["max"] = row.max
        elif(required_arg == "kills"):
            record_dict["max"] = row.kills
        else:
            record_dict["max"] = row.deaths
        if (row.name.lower() == player_name.lower()):
            result_objects.append(record_dict)
            foundMyself = True
            continue

        if (i < limit):
            result_objects.append(record_dict)

        if (foundMyself and i >= limit):
            break

    return {"data": result_objects}


@app.get("/info/{player_name}")
async def get_player_info(player_name: str):

    player_record = PlayerLog.get_or_none(
        fn.Lower(PlayerLog.name) == player_name.lower()
        )

    if player_record:
        # Calculate total kills and deaths
        total_kills = player_record.kills
        total_deaths = player_record.deaths

        # Find the maximum values for DPS, HPS, and MPS (considering both normal and 60s duration)
        max_dps = max(player_record.dps, player_record.dps60)
        max_hps = max(player_record.hps, player_record.hps60)
        max_mps = max(player_record.mps, player_record.mps60)

        return {
            "kills": total_kills,
            "deaths": total_deaths,
            "dps": max_dps,
            "hps": max_hps,
            "mps": max_mps,
        }
    else:
        return {"error": "Player not found"}


async def update_db():
    print("updating db now....")
    client = httpx.Client(http2=True)

    url = 'https://hordes.io/api/pve/getbosskillplayerlogs'
    headers = {
        'user-agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
    }

    def get_player_logs(killID):
        data = {'killid': killID, "sort": 'dps'}
        response = client.post(url, headers=headers, data=json.dumps(data))
        player_logs = response.json()

        rows = []
        for player_log in player_logs:
            row = player_log
            row["class_"] = player_log["class"]
            del row["class"]
            del row["playerid"]
            del row["bossid"]
            del row["patch"]
            del row["ohps"]
            del row["ehps"]
            del row["active"]
            del row["perfdps"]
            del row["perfhps"]
            del row["perfmit"]
            del row["dmg"]
            del row["heal"]
            del row["miti"]
            del row["casts"]
            del row["skills"]
            del row["stats"]
            del row["equip"]
            del row["dpstotal"]
            del row["hpstotal"]
            del row["mpstotal"]
            del row["gs"]
            del row["level"]

            rows.append(row)

        return rows

    def get_last_killid_fetched():
        last_killid_fetched = BossLog.select(BossLog.killid).order_by(
            BossLog.killid.desc()).limit(1).scalar()
        return last_killid_fetched or 0

    def process_player_data(player_data):
        processed_data = []

        for player_name, data in player_data.items():
            result = get_result_dict(data, player_name)
            processed_data.append(result)

        return processed_data

    def get_result_dict(data, player_name):
        max_hps_data = max((row for row in data), key=lambda x: x['hps'], default={})
        max_dps_data = max((row for row in data), key=lambda x: x['dps'], default={})
        max_mps_data = max((row for row in data), key=lambda x: x['mps'], default={})

        max_hps_data60 = max((row for row in data if row['duration'] > 60), key=lambda x: x['hps'], default={})
        max_dps_data60 = max((row for row in data if row['duration'] > 60), key=lambda x: x['dps'], default={})
        max_mps_data60 = max((row for row in data if row['duration'] > 60), key=lambda x: x['mps'], default={})

        class_ = data[0]["class_"]
        faction = data[0]["faction"]
        id = data[0]["id"]
        return {
            "id": id,
            'name': player_name,
            'hps': max_hps_data.get('hps', 0),
            'dps': max_dps_data.get('dps', 0),
            'mps': max_mps_data.get('mps', 0),
            'hpsid': max_hps_data.get('killid', 1),
            'mpsid': max_mps_data.get('killid', 1),
            'dpsid': max_dps_data.get('killid', 1),
            'faction': faction,
            'class': class_,
            'kills': len(data),
            'deaths': sum(d.get('deaths', 0) for d in data),
            'hps60': max_hps_data60.get('hps', 0),
            'dps60': max_dps_data60.get('dps', 0),
            'mps60': max_mps_data60.get('mps', 0),
            'hpsid60': max_hps_data60.get('killid', 1),
            'mpsid60': max_mps_data60.get('killid', 1),
            'dpsid60': max_dps_data60.get('killid', 1),
        }

    def dowload_player_records(last_boss_killid):
        boss_kills_threshold = 1
        boss_kills_counter = 0
        blank_killid_count = 0
        player_data = {}
        boss_rows = []
        for i in range(last_boss_killid + 1, sys.maxsize):
            try:
                print(f"id: {i} boss_kill_counter: {boss_kills_counter}")
                rows = get_player_logs(i)
                if len(rows) == 0:
                    blank_killid_count += 1
                else:
                    blank_killid_count = 0

                if blank_killid_count > 3:
                    break

                if (len(rows) > 0):
                    current_killid = rows[0]["killid"]
                    current_duration = rows[0]["duration"]
                    current_time = rows[0]["time"]

                    print(
                        f"Current killid: {current_killid} Current Duration: {current_duration} Current Time: {current_time}"
                    )
                    boss_row = {
                        "killid": current_killid,
                        "duration": current_duration,
                        "time": current_time
                    }
                    boss_rows.append(boss_row)

                # Process each player log for the current boss kill
                for row in rows:
                    player_name = row["name"]

                    # Append the row directly based on duration
                    if player_name not in player_data:
                        player_data[player_name] = []

                    # Check the duration and append the row accordingly
                    player_data[player_name].append(row)

                boss_kills_counter += 1

                if boss_kills_counter >= boss_kills_threshold:
                    # Process the organized data for every 10 boss kills
                    # print(player_data)
                    player_data = process_player_data(player_data)
                    print(player_data)
                    player_rows = []
                    existing_records = []
                    for row in player_data:
                        row["class_"] = row["class"]
                        del row["class"]
                        # Query the database to find the corresponding record
                        existing_record = PlayerLog.get_or_none(
                            (PlayerLog.name == row['name']))
                        print(f"id: {row['id']}")
                        if existing_record:
                            # Compare and update values based on conditions
                            if row['hps'] > existing_record.hps:
                                existing_record.hps = row['hps']
                                existing_record.hpsid = row['hpsid']

                            if row['dps'] > existing_record.dps:
                                existing_record.dps = row['dps']
                                existing_record.dpsid = row['dpsid']

                            if row['mps'] > existing_record.mps:
                                existing_record.mps = row['mps']
                                existing_record.mpsid = row['mpsid']


                            if row['hps60'] > existing_record.hps60:
                                existing_record.hps60 = row['hps60']
                                existing_record.hpsid60 = row['hpsid60']

                            if row['dps60'] > existing_record.dps60:
                                existing_record.dps60 = row['dps60']
                                existing_record.dpsid60 = row['dpsid60']

                            if row['mps60'] > existing_record.mps60:
                                existing_record.mps60 = row['mps60']
                                existing_record.mpsid60 = row['mpsid60']


                            # Add kills and deaths
                            existing_record.kills += row['kills']
                            existing_record.deaths += row['deaths']

                            # Save the updated record to the database
                            existing_records.append(existing_record)
                        else:
                            # No existing record, create a new one
                            player_rows.append(row)

                    with db.atomic():

                                              # print(boss_rows)
                        BossLog.insert_many(boss_rows).execute()
                        # Reset the counter and data for the next batch of boss kills
                        boss_kills_counter = 0
                        player_data = {}
                        boss_rows = []
                      
                        # Batch insert player logs
                        if player_rows:
                            # print(player_rows)
                            PlayerLog.insert_many(player_rows).execute()
                            player_rows = []
                          
                        if(existing_records):
                            PlayerLog.bulk_update(existing_records, fields=[
                                    PlayerLog.hps, PlayerLog.hpsid, PlayerLog.dps, PlayerLog.dpsid,
                                    PlayerLog.mps, PlayerLog.mpsid, PlayerLog.hps60, PlayerLog.hpsid60,
                                    PlayerLog.dps60, PlayerLog.dpsid60, PlayerLog.mps60, PlayerLog.mpsid60,
                                    PlayerLog.kills, PlayerLog.deaths
                                ])
                            existing_records = []


            except Exception as e:
                print(f"Error downloading player logs for kill id {i}: {e}")
                traceback.print_exc()
                break

    last_boss_killid = get_last_killid_fetched()
    print("Last kill id: ", last_boss_killid)

    dowload_player_records(last_boss_killid)


async def schedule_update_db():
    await update_db()
    while True:
        await asyncio.sleep(2 * 60 * 60)  # Sleep for 2 hours
        print("updating db scheduled...")
        await update_db()
        pass


def format_seconds(sec):
  minutes = sec // 60
  seconds = sec % 60
  res = ""
  if (minutes == 0):
    res = f"0:{seconds}"
  else:
    res = f"{minutes}:{seconds:02d}"
  return res


def formatTime(dt):
  dt = dt.split("T")[0]
  date_obj = datetime.datetime.strptime(dt, "%Y-%m-%d")

  # Format the datetime object
  day = date_obj.strftime("%d")  # day as 0-31
  month = date_obj.strftime("%b")  # month as full month name
  year = date_obj.strftime("%y")  # year as full four-digit number
  dt = f"{day} {month} {year}"

  return dt

if __name__ == "__main__":
    # asyncio.run(schedule_update_db()) 
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    pass
