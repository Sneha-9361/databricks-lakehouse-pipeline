import csv
import os
import random
import uuid
from datetime import datetime, timedelta

# Configuration & Seed
random.seed(42)
NUM_RECORDS = 500
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "mock_call_logs.csv")

# Reference Data Pools
AGENT_IDS = [f"AGT_{1000 + i}" for i in range(25)]
CUSTOMER_IDS = [f"CUST_{20000 + i}" for i in range(200)]

CALL_CATEGORIES = {
    "Billing Inquiry": [
        "Hi, I noticed an unexpected charge on my statement this month and need clarification.",
        "Hello, my monthly invoice seems higher than usual. Could you explain the additional fees?",
        "Good morning, I was charged twice for the same transaction last Tuesday.",
        "I need help updating my auto-pay credit card details before the next cycle.",
    ],
    "Technical Support": [
        "My internet router keeps blinking red and resetting every ten minutes.",
        "I'm unable to log into the mobile app; it keeps returning an unexpected error code.",
        "The software update crashed mid-installation and now my device won't boot.",
        "I cannot connect to the VPN network from my remote setup today.",
    ],
    "Account Management": [
        "I need to change the primary email address and phone number associated with my profile.",
        "Hello, I would like to add an authorized secondary user to my account.",
        "I recently moved and need to update my billing and service address.",
        "Can you help me reset my security questions and multi-factor authentication?",
    ],
    "Cancellation Request": [
        "I'm calling to cancel my subscription effective at the end of this billing cycle.",
        "The current plan is outside my budget, so I would like to terminate the service.",
        "I am switching to another provider and need to initiate account closure.",
        "Please cancel my policy and let me know if any early termination fees apply.",
    ],
    "Product Inquiry": [
        "I want to know the difference between the standard and enterprise service tiers.",
        "Are there any ongoing promotions or bundle discounts available for existing customers?",
        "Can you tell me if your service covers international roaming in Europe?",
        "I am interested in upgrading my hardware package. What options do I have?",
    ],
}

REASONS = list(CALL_CATEGORIES.keys())


def generate_random_timestamp(start_date: datetime, end_date: datetime) -> str:
    """Generates an ISO-formatted timestamp within a defined range."""
    time_delta = end_date - start_date
    random_seconds = random.randint(0, int(time_delta.total_seconds()))
    record_time = start_date + timedelta(seconds=random_seconds)
    return record_time.strftime("%Y-%m-%d %H:%M:%S")


def generate_mock_call_logs(num_records: int) -> list[dict]:
    """Generates synthetic call center logs with associated metadata and transcripts."""
    logs = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    for _ in range(num_records):
        reason = random.choice(REASONS)
        transcript = random.choice(CALL_CATEGORIES[reason])

        record = {
            "Call ID": str(uuid.uuid4()),
            "Timestamp": generate_random_timestamp(start_date, end_date),
            "Customer ID": random.choice(CUSTOMER_IDS),
            "Agent ID": random.choice(AGENT_IDS),
            "Call Duration": random.randint(45, 1200),  # Duration in seconds (45s to 20m)
            "Call Reason": reason,
            "Customer Transcript": transcript,
        }
        logs.append(record)

    return logs


def save_to_csv(records: list[dict], file_path: str) -> None:
    """Writes the generated mock records to a CSV file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    fieldnames = [
        "Call ID",
        "Timestamp",
        "Customer ID",
        "Agent ID",
        "Call Duration",
        "Call Reason",
        "Customer Transcript",
    ]

    with open(file_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


if __name__ == "__main__":
    logs = generate_mock_call_logs(NUM_RECORDS)
    save_to_csv(logs, OUTPUT_FILE)
    print(f"Successfully generated {len(logs)} records and saved to {OUTPUT_FILE}")