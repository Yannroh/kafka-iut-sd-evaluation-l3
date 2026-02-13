import json
import uuid
import re
import mysql.connector
from kafka import KafkaConsumer

def split_address(full_address):
    if not full_address:
        return 0, ""
    match = re.match(r"(\d+)\s+(.*)", full_address)
    if match:
        return int(match.group(1)), match.group(2)
    return 0, full_address

try:
    db_target = mysql.connector.connect(
        host='mygreaterp-db',
        user='mygreaterpuser',
        password='mygreaterppw',
        database='mygreaterp'
    )
    cursor_target = db_target.cursor()

    

except mysql.connector.Error as err:
    print(f"Database connection error: {err}")
    exit(1)

consumer = KafkaConsumer(
    'eurynome.eurynome.customer',
    bootstrap_servers=['my-kafka.rohmer12u-dev.svc.cluster.local:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='sync-eurynome-erp-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    if not message.value:
        continue

    payload = message.value.get('payload')
    if not payload:
        continue

    op = payload.get('op')
    after = payload.get('after')

    if op in ['c', 'r'] and after:
        eurynome_id = after.get('id')

        cursor_ref.execute("SELECT mygreaterp_id FROM customers_ref WHERE eurynome_id = %s", (eurynome_id,))
        if cursor_ref.fetchone():
            continue

        uuid_contact = str(uuid.uuid4())
        uuid_address = str(uuid.uuid4())
        uuid_link = str(uuid.uuid4())

        prenom = after.get('prenom')
        nom = after.get('nom')
        
        raw_address = after.get('adresse_rue', '')
        num, street = split_address(raw_address)
        city = after.get('adresse_ville')
        region = after.get('adresse_region')

        try:
            sql_contact = "INSERT INTO contacts (id, first_name, last_name) VALUES (%s, %s, %s)"
            cursor_target.execute(sql_contact, (uuid_contact, prenom, nom))

            sql_address = "INSERT INTO addresses (id, number, street, city, state) VALUES (%s, %s, %s, %s, %s)"
            cursor_target.execute(sql_address, (uuid_address, num, street, city, region))

            sql_link = "INSERT INTO contacts_addresses (id, contact_id, address_id) VALUES (%s, %s, %s)"
            cursor_target.execute(sql_link, (uuid_link, uuid_contact, uuid_address))

            sql_ref = "INSERT INTO customers_ref (eurynome_id, mygreaterp_id) VALUES (%s, %s)"
            cursor_ref.execute(sql_ref, (eurynome_id, uuid_contact))

            db_target.commit()
            

        except mysql.connector.Error as e:
            db_target.rollback()
            print(f"SQL Error: {e}")