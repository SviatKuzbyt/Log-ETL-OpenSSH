import psycopg2
import os
from dotenv import load_dotenv
import re
from datetime import datetime

def set_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT")
    )

def get_data(cursor, table):
        query = f"SELECT * FROM {table}"
        cursor.execute(query)
        return cursor.fetchall()

def convert_date(year, month, day, time):
    date_str = f"{year} {month} {day} {time}"
    date_converted = datetime.strptime(date_str, "%Y %b %d %H:%M:%S")
    return date_converted.strftime("%Y-%m-%d %H:%M:%S")

def get_event_id(events_templates, content):
    for template in events_templates:
        t_id = template[0]
        t_text = template[1]
    
        if not t_text or t_text == 'Other':
            continue
                
        log_match = re.search(t_text, content)
        if log_match:
            return t_id
        
    return 28 #default

def get_component(component_name, components):
    for component in components:
        if component_name == component[1]:
            return component[0]
    return 2 #default

def convert_log(log_dict, events_templates, components):
    date = convert_date(2025, log_dict['month'], log_dict['day'], log_dict['time'])
    event_id = get_event_id(events_templates, log_dict["content"].strip())
    component_id = get_component(log_dict['component'], components)

    return {
        "date": date,
        "pid": int(log_dict["pid"]),
        "content": log_dict["content"],
        "event_id": event_id,
        "component_id": component_id
    }

def insert_log(cursor, log_converted):
    query = "INSERT INTO logs(log_date, log_pid, log_content, event_id, component_id) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(query, (
        log_converted["date"], 
        log_converted["pid"], 
        log_converted["content"], 
        log_converted["event_id"],
        log_converted["component_id"]
    )) 

load_dotenv()
events_templates = []
components = []
pattern = r"(?P<month>\w{3})\s+(?P<day>\d+)\s+(?P<time>\d{2}:\d{2}:\d{2})\s+(?P<component>\w+)\s+sshd\[(?P<pid>\d+)\]:\s+(?P<content>.*)"
log_path = "SSH.log" # SET LOG HERE

try:
    conn = set_connection()
    
    with conn.cursor() as cursor:
        print("Moving data...") 
        events_templates = get_data(cursor, "events")
        components = get_data(cursor, "components") 

        with open(log_path) as logs:
            for log in logs:
                log_match = re.search(pattern, log)
                if log_match:
                    log_dict = log_match.groupdict()
                    log_converted = convert_log(log_dict, events_templates, components)
                    insert_log(cursor, log_converted)

        conn.commit()
        print("All data has been successfully moved!")            
except Exception as error:
    print(f"Error: {error}")
finally:
    if conn:
        conn.close()

#TODO: Improve the events table to search for all events 