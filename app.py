from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka_admin import KafkaManager

from models.loan import Loan, db

import threading
import json
import os

app = Flask(__name__)

# Setup db
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///app.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)

kafka_manager = KafkaManager()

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Kafka consumer logic
def safe_deserializer(m):
    if m is None:
        return None
    try:
        return json.loads(m.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        return None

def loan_event_consumer():
    consumer = KafkaConsumer(
        "loan-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='loan-consumer-group',
        auto_offset_reset='earliest',
        value_deserializer=safe_deserializer
    )

    for message in consumer:
        data = message.value
        if data is None:
            continue

        print(f"[Loan Events Consumer] Received: {data}")

        if data['status'] == "approved":
            producer.send('loan-approvals', value=data)
            producer.flush()
            print(f"[Loan Events Consumer] Sent {data["loan_id"]} to approvals topic.")
        elif data['status'] == "rejected":
            producer.send('loan-rejections', value=data)
            producer.flush()
            print(f"[Loan Events Consumer] Sent {data['loan_id']} to rejections topic.]")
        else:
            print(f"[Loan Events Consumer] Received invalid message: {data}")
            # Send to DLT since status is unrecognizable

def loan_approvals_consumer():
    with app.app_context():
        consumer = KafkaConsumer(
            "loan-approvals",
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='loan-consumer-group',
            auto_offset_reset='earliest',
            value_deserializer=safe_deserializer
        )

        for message in consumer:
            data = message.value
            if data is None:
                continue

            print(f"[Loan Approvals Consumer] Received: {data}")

            if data['status'] == "approved":
                loan = Loan.query.filter_by(loan_id=data['loan_id']).first()

                if not loan:
                    print(f"[Loan Approvals Consumer] Could not retrieve loan with ID {data['loan_id']}")
                    continue

                loan.status = data['status']
                db.session.commit()
                print(f"[Loan Approval Consumer] Updated {data["loan_id"]} in database to approved.")
            else:
                print(f"[Loan Approval Consumer] Received invalid message: {data}")
                continue
                # Send to DLT because status did not match up

def loan_rejections_consumer():
    with app.app_context():
        consumer = KafkaConsumer(
            "loan-rejections",
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='loan-consumer-group',
            auto_offset_reset='earliest',
            value_deserializer=safe_deserializer
        )

        for message in consumer:
            data = message.value
            if data is None:
                continue

            print(f"[Loan Approvals Consumer] Received: {data}")

            if data['status'] == "rejected":
                loan = Loan.query.filter_by(loan_id=data['loan_id']).first()

                if not loan:
                    print(f"[Loan Approvals Consumer] Could not retrieve loan with ID {data['loan_id']}")
                    continue

                loan.status = data['status']
                db.session.commit()

                print(f"[Loan Approval Consumer] Updated {data["loan_id"]} in database to rejected.")
            else:
                print(f"[Loan Rejection Consumer] Received invalid message: {data}")
                continue
                # Send to DLT because status did not match up

# Start consumer in a background thread
def start_background_consumer():
    print("🟢 Starting background consumer...")
    for topic in kafka_manager.admin_client.list_topics():
        if topic == "loan-events":
            consumer_thread = threading.Thread(target=loan_event_consumer, daemon=True)
            consumer_thread.start()
        elif topic == "loan-approvals":
            consumer_thread = threading.Thread(target=loan_approvals_consumer, daemon=True)
            consumer_thread.start()
        else:
            consumer_thread = threading.Thread(target=loan_rejections_consumer, daemon=True)
            consumer_thread.start()

    print(f"🟢 Started threads for topics: {kafka_manager.admin_client.list_topics()}")

# Create Kafka Topics to go through
def create_kafka_topics():
    topics = [
        {'name': 'loan-events', 'partitions': 3, 'replication_factor': 1},
        {'name': 'loan-approvals', 'partitions': 3, 'replication_factor': 1},
        {'name': 'loan-rejections', 'partitions': 3, 'replication_factor': 1}
    ]
    kafka_manager.create_topics(topics)

with app.app_context():
    create_kafka_topics()
    start_background_consumer()
    print("🟡 Kafka Consumers are listening...")

    if not os.path.exists("app.db"):
        with app.app_context():
            db.create_all()
            print("🟢 Database created.")

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Routes
@app.route('/publish/loan-events', methods=['POST'])
def publish_loan_events():
    data = request.json
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    producer.send('loan-events', value=data)
    producer.flush()
    return jsonify({'message': 'Data sent to Kafka for loan-events'}), 200

@app.route("/loan-status/<int:db_id>", methods=['GET'])
def get_loan_status(db_id):
    loan = Loan.query.filter_by(id=db_id).first()
    if loan:
        return jsonify({"id": loan.id,
                        "loan_id": loan.loan_id,
                        "applicant_name": loan.applicant_name,
                        "loan_amount": loan.loan_amount,
                        "status": loan.status})
    else:
        return jsonify([])

@app.route('/submit-loan', methods=['PUT'])
def submit_loan():
    data = request.get_json()
    loan = Loan.query.filter_by(loan_id=data['loan_id']).first()
    if not loan:
        new_loan = Loan(loan_id=data['loan_id'],
                        applicant_name=data['applicant_name'],
                        loan_amount=data['loan_amount'],
                        status="pending")

        db.session.add(new_loan)
        db.session.commit()
        info_string = f"Loan: {data['loan_id']} with DB ID {new_loan.id} created."
        return jsonify({"message": info_string}), 201
    else:
        return jsonify({'message': 'Loan with same loan id already submitted'}), 400

if __name__ == '__main__':
    app.run(debug=False,port=5000)