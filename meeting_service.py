# meeting_service.py
import os
import uuid
import math
import base64
import decimal
import hmac
import hashlib
import re
import pathlib
import threading
import time as time_module
import boto3
import redis
import json
import urllib.parse
import snowflake.connector
import traceback
import requests as http_requests  # renamed to avoid clash with FastAPI
import numpy as np
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime, timedelta, timezone
from datetime import time as dt_time
from typing import Optional, List, Dict, Any, Tuple
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi.requests import Request
from fastapi import FastAPI, HTTPException, Header, Depends, status, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, EmailStr
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from snowflake.connector import errors
from sklearn.linear_model import Ridge, LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import r2_score, accuracy_score
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi import Form
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from boto3.dynamodb.conditions import Key, Attr

# Load .env FIRST so os.getenv() calls below pick up .env values
load_dotenv(dotenv_path=pathlib.Path(__file__).parent / ".env", override=False)

# Local monorepo fallback: if Meeting-Git/.env is absent, pull shared secrets
# (e.g., Razorpay keys) from Billing-Git/.env without overriding existing vars.
load_dotenv(dotenv_path=pathlib.Path(__file__).parent.parent / "Billing-Git" / ".env", override=False)

# ================= SERVICE URLS =================

HOME_SERVICE_URL        = os.getenv("HOME_SERVICE_URL",        "http://127.0.0.1:5001")
CHATBOT_SERVICE_URL     = os.getenv("CHATBOT_SERVICE_URL",     "http://127.0.0.1:7600")
ASSET_SERVICE_URL       = os.getenv("ASSET_SERVICE_URL",       "http://127.0.0.1:8090")
INTERNSHIP_SERVICE_URL  = os.getenv("INTERNSHIP_SERVICE_URL",  "http://127.0.0.1:5050")
MS365_SERVICE_URL       = os.getenv("MS365_SERVICE_URL",       "http://127.0.0.1:7700")
EMPLOYEE_SERVICE_URL    = os.getenv("EMPLOYEE_SERVICE_URL",    "http://127.0.0.1:8002")
BLOGGER_SERVICE_URL     = os.getenv("BLOGGER_SERVICE_URL",     "http://127.0.0.1:7500")
REDIS_SERVICE_URL       = os.getenv("REDIS_SERVICE_URL",       "http://127.0.0.1:6380")
BRS_SERVICE_URL         = os.getenv("BRS_SERVICE_URL",         "http://127.0.0.1:8020")
BILLING_SERVICE_URL     = os.getenv("BILLING_SERVICE_URL",     "http://127.0.0.1:8010")
RAG_SERVICE_URL         = os.getenv("RAG_SERVICE_URL",         "http://127.0.0.1:7900")
STUDENT_SERVICE_URL     = os.getenv("STUDENT_SERVICE_URL",     "http://127.0.0.1:8030")
LAMBDA_URL = 'https://lwug4xhfz27whiuu3acjfwsgtm0ttwja.lambda-url.eu-north-1.on.aws/'
STATIC_CDN = "https://d1pjjckqswt5z7.cloudfront.net"

CANONICAL_HOST = os.getenv("CANONICAL_HOST","www.chakorahub.com").strip().lower()
INTERNSHIP_PUBLIC_HOST = os.getenv("INTERNSHIP_PUBLIC_HOST","api.chakorahub.com").strip().lower()

app = FastAPI(title="meeting_service")

# ------------------------------
# MS365 MICROSERVICE CLIENT
# ------------------------------
MS_ORGANIZER = os.getenv("MS_ORGANIZER", "support@chakorahub.com")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# DynamoDB setup
dynamodb = boto3.resource('dynamodb', region_name='eu-north-1')
bookings_table = dynamodb.Table('Bookings')

# ── Kafka Producer ──────────────────────────────────────────────
_kafka_producer = None
try:
    _kafka_producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3
    )
    print("✅ Kafka producer connected (meeting_service)")
except Exception as _ke:
    print(f"⚠️  Kafka producer unavailable (meeting_service): {_ke}")

def _kafka_publish(topic: str, payload: dict) -> None:
    """Fire-and-forget. Falls back silently if Kafka is down."""
    if _kafka_producer is None:
        print(f"⚠️  Kafka publish skipped [{topic}] because producer is unavailable")
        return
    try:
        print(f"📤 Kafka publish request → {topic} | keys={list(payload.keys())}")
        _kafka_producer.send(topic, value=payload)
        _kafka_producer.flush(timeout=2)
        print(f"📤 Kafka → {topic}: {payload}")
    except Exception as e:
        print(f"⚠️  Kafka publish failed [{topic}]: {e}")

def _verify_razorpay_signature(order_id: str, payment_id: str, signature: str) -> bool:
    secret = (os.getenv("RZP_KEY_SECRET") or os.getenv("RAZORPAY_KEY_SECRET") or "").strip()
    if not secret:
        print("⚠️ Razorpay secret missing in meeting_service")
        return False
    message = f"{order_id}|{payment_id}".encode("utf-8")
    expected = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

def _consume_teams_link_created() -> None:
    """Consume teams.link.created and persist Teams link in DynamoDB."""
    try:
        consumer = KafkaConsumer(
            "teams.link.created",
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="meeting-teams-link-created-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        print("✅ Kafka consumer listening on: teams.link.created")
    except Exception as exc:
        print(f"⚠️ Kafka consumer failed to start (teams.link.created): {exc}")
        return

    for message in consumer:
        print(f"📥 Kafka consume ← {message.topic} | partition={message.partition} offset={message.offset}")
        event = message.value or {}
        booking_id = event.get("booking_id")
        teams_link = event.get("teams_link") or event.get("meeting_link")
        incoming_meeting_id = (event.get("meeting_id") or "").strip()

        if not booking_id or not teams_link:
            print(f"⚠️ Invalid teams.link.created event: {event}")
            continue

        try:
            if incoming_meeting_id:
                bookings_table.update_item(
                    Key={"bookingId": booking_id},
                    UpdateExpression="""
                        SET teams_link = :tl,
                            teams_link_created_at = :ts,
                            meeting_id = :mid,
                            transcript_status = :status,
                            transcript_s3_key = :s3key
                    """,
                    ExpressionAttributeValues={
                        ":tl": teams_link,
                        ":ts": datetime.utcnow().isoformat(),
                        ":mid": incoming_meeting_id,
                        ":status": "PENDING",
                        ":s3key": ""
                    },
                )
                print(f"✅ Teams link updated with meeting_id | booking_id={booking_id} | meeting_id={incoming_meeting_id}")
            else:
                bookings_table.update_item(
                    Key={"bookingId": booking_id},
                    UpdateExpression="""
                        SET teams_link = :tl,
                            teams_link_created_at = :ts,
                            transcript_status = :status,
                            transcript_s3_key = :s3key
                    """,
                    ExpressionAttributeValues={
                        ":tl": teams_link,
                        ":ts": datetime.utcnow().isoformat(),
                        ":status": "PENDING",
                        ":s3key": ""
                    },
                )
                print(f"⚠️ Teams link updated without meeting_id (preserving existing DB meeting_id) | booking_id={booking_id}")
            print(f"✅ Teams link updated | booking_id={booking_id}")
        except Exception as exc:
            print(f"❌ Failed to update Teams link | booking_id={booking_id} | error={exc}")


def _consume_suggestion_events() -> None:
    """Consume suggestion pipeline completion events and cache latest per student."""
    try:
        consumer = KafkaConsumer(
            "meeting.suggestion.completed",
            "agent.recommendation.created",
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="meeting-suggestion-events-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        print("✅ Kafka consumer listening on: meeting.suggestion.completed, agent.recommendation.created")
    except Exception as exc:
        print(f"⚠️ Kafka consumer failed to start (suggestion events): {exc}")
        return

    for message in consumer:
        print(f"📥 Kafka consume ← {message.topic} | partition={message.partition} offset={message.offset}")
        event = message.value or {}
        student_email = (event.get("student_email") or event.get("email") or "").strip().lower()
        if not student_email:
            print(f"⚠️ Invalid suggestion event payload: {event}")
            continue
        try:
            cache_key = f"meeting:suggestion:latest:{student_email}"
            cache_payload = {
                "topic": message.topic,
                "event": event,
                "cached_at": datetime.utcnow().isoformat(),
            }
            _rs_set(cache_key, json.dumps(cache_payload), 3600)
            print(f"✅ Suggestion event cached | topic={message.topic} | student={student_email}")
        except Exception as exc:
            print(f"❌ Failed to cache suggestion event | topic={message.topic} | student={student_email} | {exc}")

# ======================== NEW: FEEDBACK TRIGGER BACKGROUND THREAD =========================
POLL_INTERVAL_SECONDS = 60   # check every minute

def _process_completed_bookings():
    """
    Background thread: poll DynamoDB for completed bookings without feedback sent,
    and call student_service to generate feedback links.
    """
    while True:
        try:
            # Scan for bookings with status = 'COMPLETED' and feedback_sent missing or false
            response = bookings_table.scan(
                FilterExpression=(
                    Attr('status').eq('COMPLETED') &
                    (Attr('feedback_sent').not_exists() | Attr('feedback_sent').eq(False))
                )
            )
            items = response.get('Items', [])
            print(f"📋 Found {len(items)} completed bookings without feedback sent.")

            for item in items:
                booking_id = item['bookingId']
                student_email = item.get('student_email', '')
                student_name = item.get('created_by', 'Student')  # fallback

                if not student_email:
                    print(f"⚠️ Skipping booking {booking_id}: no student_email")
                    continue

                # Call student_service to generate feedback
                try:
                    resp = http_requests.post(
                        f"{STUDENT_SERVICE_URL}/api/student/feedback/generate",
                        json={
                            "meeting_id": booking_id,
                            "student_email": student_email,
                            "student_name": student_name,
                            "booking_id": booking_id,
                            "expiry_hours": 24.0
                        },
                        timeout=10
                    )
                    if resp.status_code == 200:
                        print(f"✅ Feedback triggered for booking {booking_id}")
                        # Update DynamoDB to mark feedback sent
                        bookings_table.update_item(
                            Key={'bookingId': booking_id},
                            UpdateExpression="SET feedback_sent = :sent, feedback_generated_at = :ts",
                            ExpressionAttributeValues={
                                ':sent': True,
                                ':ts': datetime.utcnow().isoformat()
                            }
                        )
                    else:
                        print(f"⚠️ Feedback API error for {booking_id}: {resp.status_code} - {resp.text}")
                except Exception as e:
                    print(f"❌ Failed to call feedback generation for {booking_id}: {e}")

            # Sleep before next poll
            time_module.sleep(POLL_INTERVAL_SECONDS)
        except Exception as e:
            print(f"❌ Error in _process_completed_bookings: {e}")
            time_module.sleep(POLL_INTERVAL_SECONDS)
# ========================================================================================


# ------------------------------
# ENV & AWS CLIENTS
# ------------------------------
BOOKINGS_TABLE = os.getenv("BOOKINGS_TABLE", "Bookings")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@chakorahub.com")
ADMIN_PANEL_URL = os.getenv(
    "ADMIN_PANEL_URL",
    "https://www.chakorahub.com/meeting/admin"
)

boto_config = Config(region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", config=boto_config)
ses = boto3.client("ses", region_name=AWS_REGION)
bookings_table = dynamodb.Table(BOOKINGS_TABLE)

# Employees

EMPLOYEE_EMAILS = [ 
    "support@chakorahub.com",
    "sathvika@chakorahub.com",
    "saitejatatineni@chakorahub.com",
    "Prathibha@chakorahub.com",
    "poojitha@chakorahub.com",
    "Mahesh@chakorahub.com",
    "ganeshneeli@chakorahub.com",
    "Bhavishya@chakorahub.com",
    "anupamamekala@chakorahub.com",
    "dhruvakoushik@chakorahub.com",
    "prasad@chakorahub.com"
]

VALID_PURPOSES = [
    "Technical Support",
    "Code Review",
    "Architecture Discussion",
    "Project Planning",
    "Performance Issues",
    "Bug Resolution",
    "Feature Discussion",
    "Career Guidance",
    "Other"
]

# --- ADD THESE TO YOUR CONSTANTS SECTION ---
TOTAL_STAFF = len(EMPLOYEE_EMAILS) # team size for utilization heuristics
DEMAND_MULTIPLIER = 0.05  # Increase price by 5% for every existing booking that day
SUPPLY_PENALTY = 50       # Extra charge if more than 70% of staff are busy
EARLY_BIRD_DISCOUNT = 0.92 # 8% discount if booking 7-13 days in advance

# Heuristic factor controls (phase-2 tuning)
MAX_DEMAND_FACTOR = 1.75
PEAK_HOUR_MULTIPLIER = 1.08
OFFPEAK_HOUR_DISCOUNT = 0.95

CHARSET = "UTF-8"

BASE_RATE = 575
COMPLEXITY_MULTIPLIER = {"Easy": 0.875, "Medium": 1.0, "Difficult": 1.125}
MINIMUM_CHARGE_EXISTING = 50
INTERNAL_DOMAIN_PRICE = 1

VALID_CREDENTIALS = {
    "student": "student",
    "admin": "admin",
}

BUSINESS_START = dt_time(10, 0)
BUSINESS_END = dt_time(19, 0)
GRID_MINUTES = 15
EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


# ------------------------------
# FASTAPI APP & CORS
# ------------------------------
app = FastAPI(title="ChakoraHub Meeting Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://www.chakorahub.com"],   # development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBasic()


@app.on_event("startup")
def start_teams_link_consumer() -> None:
    """Start Kafka consumer threads used by meeting flow."""
    t = threading.Thread(target=_consume_teams_link_created, daemon=True, name="kafka-teams-link-created")
    t.start()
    print("🚀 Kafka consumer thread started: teams.link.created")

    t2 = threading.Thread(target=_consume_suggestion_events, daemon=True, name="kafka-suggestion-events")
    t2.start()
    print("🚀 Kafka consumer thread started: meeting.suggestion.completed, agent.recommendation.created")

    # NEW: start the completed bookings processor
    t3 = threading.Thread(target=_process_completed_bookings, daemon=True, name="completed-bookings-processor")
    t3.start()
    print("🚀 Background thread started: completed-bookings-processor")
# ------------------------------
# REDIS CONFIGURATION (Direct client — fallback for internal ops only)
# The meeting module uses redis_service HTTP API for all locking / availability.
# This direct client is kept ONLY for the pricing-model cache that does NOT
# need per-slot locking semantics.
# ------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

# DB 8 is reserved for the meeting service (availability + locking + pending holds)
# All meeting-specific keys go through redis_service HTTP endpoints /meeting/...
# The direct Redis client below is DB 0 (pricing model cache only).
def create_redis_client() -> Optional[redis.Redis]:
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
            retry_on_timeout=True,
            health_check_interval=30,
        )
        client.ping()
        print(
            f"✅ Redis connected | host={REDIS_HOST} port={REDIS_PORT} db={REDIS_DB} "
            f"password_set={bool(REDIS_PASSWORD)}"
        )
        return client
    except Exception as exc:
        print(f"⚠️ Redis unavailable, continuing without cache: {exc}")
        return None


redis_client = create_redis_client()


def redis_get_safe(key: str) -> Optional[str]:
    if redis_client is None:
        return None
    try:
        return redis_client.get(key)
    except Exception as exc:
        print(f"⚠️ Redis GET failed for {key}: {exc}")
        return None


def redis_setex_safe(key: str, ttl: int, value: str) -> bool:
    if redis_client is None:
        return False
    try:
        redis_client.setex(key, ttl, value)
        return True
    except Exception as exc:
        print(f"⚠️ Redis SETEX failed for {key}: {exc}")
        return False


def redis_delete_safe(key: str) -> bool:
    if redis_client is None:
        return False
    try:
        redis_client.delete(key)
        return True
    except Exception as exc:
        print(f"⚠️ Redis DELETE failed for {key}: {exc}")
        return False

# Cache TTLs from your Phase 4 Architecture diagram
TTL_EMPLOYEES = 3600    # 1 Hour
TTL_PURPOSES = 86400    # 24 Hours
TTL_SLOTS = 300         # 5 Minutes
TTL_PRICING_MODEL = 1800  # 30 Minutes

PRICING_MODEL_CACHE_KEY = "meeting:pricing:model"
PRICING_MODEL_VERSION = "linear-regression-v1"
MIN_TRAINING_SAMPLES = 5
RIDGE_REGULARIZATION = 0.05
MAX_MODEL_PRICE_DELTA = 0.20

# ── AUTO SUGGESTION MODEL CONSTANTS ──────────────────────────────────────────
# Three-model ensemble: Ridge (duration), LogisticRegression (complexity),
# RandomForest (complexity) — same stack as the deployment diagram.

AUTO_SUGGESTION_CACHE_KEY        = "meeting:autosuggestion:model"
AUTO_SUGGESTION_MODEL_VERSION    = "auto-suggestion-v1"
AUTO_SUGGESTION_CACHE_TTL        = 3600   # 1 hour — suggestions change slower than prices
AUTO_SUGGESTION_MIN_SAMPLES      = 10     # need more samples for 3-model ensemble
BGE_EMBEDDING_CACHE_PREFIX       = "meeting:embed:"
BGE_EMBEDDING_CACHE_TTL          = 86400  # 24 hours — embeddings are stable

# Complexity label encoding (must stay consistent between train & predict)
COMPLEXITY_LABELS  = ["Easy", "Medium", "Difficult"]
COMPLEXITY_ENCODER = LabelEncoder()
COMPLEXITY_ENCODER.fit(COMPLEXITY_LABELS)

# Confidence thresholds
DURATION_MAX_DELTA_FACTOR  = 0.30   # Ridge duration suggestion capped ±30% of median
COMPLEXITY_MIN_CONFIDENCE  = 0.55   # below this RF/LR confidence → return None


# ──────────────────────────────────────────────────────────────────────────────
# REDIS-SERVICE HTTP HELPERS  (DB 8 — meeting: namespace)
# ──────────────────────────────────────────────────────────────────────────────
# All slot availability, locking, and pending-hold operations are delegated to
# the shared redis_service microservice (port 6380).  This keeps the Redis
# logic decoupled from the meeting module exactly as requested.
#
# Key schema (DB 8):
#   meeting:slots:{date}              → JSON list of all slot dicts with availability
#   meeting:lock:slot:{slot_key}      → short TTL lock (15–30 sec) prevents double-booking
#   meeting:pending:{auth_identifier} → temporary hold (TTL 90 sec) while payment flows
#   meeting:user:{auth_identifier}    → short TTL user booking summary (read acceleration)
# ──────────────────────────────────────────────────────────────────────────────

# DB index used inside redis_service for meeting-specific keys
_MEETING_DB = 8

MEETING_SLOTS_KEY_PREFIX = "meeting:slots:"
MEETING_SLOT_LOCK_KEY_PREFIX = "meeting:lock:slot:"
MEETING_PENDING_HOLD_KEY_PREFIX = "meeting:pending:"
MEETING_USER_CACHE_KEY_PREFIX = "meeting:user:"
MEETING_STUDENT_KEY_PREFIX = "meeting:student:"

# TTLs for meeting-specific Redis keys
TTL_SLOT_LOCK    = 120    # seconds — short lock while payment is being processed
TTL_PENDING_HOLD = 180    # seconds — temporary hold while Razorpay checkout is open
TTL_SLOT_AVAIL   = 300   # seconds — availability cache (5 min), same as TTL_SLOTS
TTL_USER_CACHE   = 120   # seconds — per-user booking list (2 min)


def _meeting_slots_key(date: str) -> str:
    return f"{MEETING_SLOTS_KEY_PREFIX}{date}"


def _meeting_slot_lock_key(slot_key: str) -> str:
    return f"{MEETING_SLOT_LOCK_KEY_PREFIX}{slot_key}"


def _meeting_pending_hold_key(auth_identifier: str) -> str:
    return f"{MEETING_PENDING_HOLD_KEY_PREFIX}{auth_identifier}"


def _meeting_user_cache_key(auth_identifier: str) -> str:
    return f"{MEETING_USER_CACHE_KEY_PREFIX}{auth_identifier}"


def _meeting_student_key(auth_identifier: str) -> str:
    return f"{MEETING_STUDENT_KEY_PREFIX}{auth_identifier}"


def _rs_get(key: str) -> Optional[Any]:
    """GET a key from redis_service (meeting DB)."""
    try:
        resp = http_requests.get(
            f"{REDIS_SERVICE_URL}/redis/get",
            params={"key": key, "db": _MEETING_DB},
            timeout=3,
        )
        data = resp.json()
        if data.get("success") and data.get("found"):
            return data["value"]
        return None
    except Exception as exc:
        print(f"⚠️ redis_service GET {key} failed: {exc}")
        return None


def _rs_set(key: str, value: str, ttl: int) -> Optional[bool]:
    """SETEX a key in redis_service (meeting DB)."""
    try:
        resp = http_requests.post(
            f"{REDIS_SERVICE_URL}/redis/set",
            json={"key": key, "value": value, "db": _MEETING_DB, "ttl": ttl},
            timeout=3,
        )
        return resp.json().get("success", False)
    except Exception as exc:
        print(f"⚠️ redis_service SET {key} failed: {exc}")
        return None


def _rs_delete(keys: List[str]) -> bool:
    """DELETE one or more keys from redis_service (meeting DB)."""
    if not keys:
        return True
    try:
        resp = http_requests.post(
            f"{REDIS_SERVICE_URL}/redis/delete",
            json={"keys": keys, "db": _MEETING_DB},
            timeout=3,
        )
        return resp.json().get("success", False)
    except Exception as exc:
        print(f"⚠️ redis_service DELETE {keys} failed: {exc}")
        return False


def _rs_exists(key: str) -> Optional[bool]:
    """Check key existence via redis_service (meeting DB)."""
    try:
        resp = http_requests.get(
            f"{REDIS_SERVICE_URL}/redis/exists",
            params={"key": key, "db": _MEETING_DB},
            timeout=3,
        )
        return resp.json().get("exists", False)
    except Exception as exc:
        print(f"⚠️ redis_service EXISTS {key} failed: {exc}")
        return None


# ── Slot availability cache ────────────────────────────────────────────────────

def meeting_slots_cache_get(date: str) -> Optional[dict]:
    """Return cached slot availability for a date, or None on miss."""
    raw = _rs_get(_meeting_slots_key(date))
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def meeting_slots_cache_set(date: str, payload: dict) -> None:
    """Cache slot availability for a date (TTL 5 min)."""
    _rs_set(_meeting_slots_key(date), json.dumps(payload), TTL_SLOT_AVAIL)


def meeting_slots_cache_invalidate(date: str) -> bool:
    """Evict slot availability cache after a booking or cancellation."""
    try:
        result = _rs_delete([_meeting_slots_key(date)])
        if result:
            print(f"✅ Redis: Slot cache invalidated | date={date}")
        else:
            print(f"⚠️ Redis: Slot cache invalidation returned False | date={date}")
        return result
    except Exception as e:
        print(f"❌ Redis: Slot cache invalidation failed | date={date} error={e}")
        return False


# ── Slot locking (prevent double-booking) ─────────────────────────────────────

def meeting_slot_lock_acquire(slot_key: str, holder: str) -> bool:
    """
    Acquire a short-TTL lock for a specific slot.

    slot_key : "{date}:{start_time}:{duration_minutes}"  e.g. "2025-08-01:10:30:60"
    holder   : user identifier (username / booking_id)

    Returns True if the lock was acquired, False if already held.

    Implementation note:
      redis_service exposes only SET (with ttl) and EXISTS.  We simulate SETNX by
      checking EXISTS first then SET.  There is a tiny race window, but for a
      booking flow (where Razorpay payment is the authoritative serialisation point)
      this is acceptable.  A future upgrade can add a native /redis/setnx endpoint
      to redis_service to make this fully atomic.
    """
    lock_key = _meeting_slot_lock_key(slot_key)
    exists = _rs_exists(lock_key)
    if exists is None:
        raise RuntimeError("redis_service unavailable during lock existence check")
    if exists:
        print(f"ℹ️ Slot lock acquire blocked | slot={slot_key} holder={holder} reason=exists_true")
        return False   # already locked by another request
    set_ok = _rs_set(lock_key, holder, TTL_SLOT_LOCK)
    if set_ok is None:
        raise RuntimeError("redis_service unavailable during lock write")
    if not set_ok:
        print(f"⚠️ Slot lock acquire set failed | slot={slot_key} holder={holder}")
    return set_ok


def meeting_slot_lock_release(slot_key: str) -> None:
    """Release the slot lock (called after DynamoDB write succeeds or on failure)."""
    _rs_delete([_meeting_slot_lock_key(slot_key)])


def meeting_slot_lock_held_by(slot_key: str) -> Optional[str]:
    """Return who holds the lock, or None if unlocked."""
    return _rs_get(_meeting_slot_lock_key(slot_key))


# ── Pending hold (temporary reservation during payment checkout) ───────────────

def meeting_pending_hold_set(auth_identifier: str, hold_data: dict) -> None:
    """
    Store a pending hold for a user while they complete Razorpay checkout.
    TTL 90 sec — if payment is not completed in time the hold expires automatically.
    """
    _rs_set(_meeting_pending_hold_key(auth_identifier), json.dumps(hold_data), TTL_PENDING_HOLD)


def meeting_pending_hold_get(auth_identifier: str) -> Optional[dict]:
    """Retrieve the current pending hold for an authenticated meeting user."""
    raw = _rs_get(_meeting_pending_hold_key(auth_identifier))
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def meeting_pending_hold_clear(auth_identifier: str) -> None:
    """Clear the pending hold after booking is confirmed or cancelled."""
    _rs_delete([_meeting_pending_hold_key(auth_identifier)])


def meeting_pending_hold_matches_slot(
    hold: Optional[dict],
    *,
    date: str,
    start_time: str,
    duration_minutes: int,
) -> bool:
    """Return True when a pending hold still matches the slot being booked."""
    if not hold:
        return False

    try:
        hold_duration = int(hold.get("duration_minutes", 0) or 0)
    except (TypeError, ValueError):
        hold_duration = 0

    return (
        hold.get("date") == date
        and hold.get("start_time") == start_time
        and hold_duration == int(duration_minutes)
    )


# ── Per-user booking list cache (read acceleration) ───────────────────────────

def meeting_user_cache_set(auth_identifier: str, bookings: list) -> None:
    """Cache a user's booking list for fast repeated reads (TTL 2 min)."""
    _rs_set(_meeting_user_cache_key(auth_identifier), json.dumps(bookings), TTL_USER_CACHE)


def meeting_user_cache_get(auth_identifier: str) -> Optional[list]:
    raw = _rs_get(_meeting_user_cache_key(auth_identifier))
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def meeting_user_cache_invalidate(auth_identifier: str) -> None:
    _rs_delete([_meeting_user_cache_key(auth_identifier)])


def meeting_student_marker_get(auth_identifier: str) -> Optional[dict]:
    """Persistent marker used to quickly identify returning students in DB 8."""
    if not auth_identifier:
        return None
    raw = _rs_get(_meeting_student_key(auth_identifier))
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def meeting_student_marker_set(auth_identifier: str, marker: dict) -> bool:
    """Store a no-TTL student marker so DB 8 has durable per-student identity keys."""
    if not auth_identifier:
        return False
    try:
        resp = http_requests.post(
            f"{REDIS_SERVICE_URL}/redis/set",
            json={
                "key": _meeting_student_key(auth_identifier),
                "value": json.dumps(marker),
                "db": _MEETING_DB,
            },
            timeout=3,
        )
        ok = resp.json().get("success", False)
        if ok:
            print(f"✅ Redis: Student marker updated | user={auth_identifier}")
        else:
            print(f"⚠️ Redis: Student marker update returned False | user={auth_identifier}")
        return ok
    except Exception as exc:
        print(f"⚠️ Redis: Student marker update failed | user={auth_identifier} error={exc}")
        return False


# Helper to always return a fresh DB connection with RSA key authentication
def get_db_connection():
    try:
        # Load RSA private key
        # Get the directory where meeting_service.py is located
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        key_path = os.path.join(BASE_DIR, 'rsa_key.p8')

        with open(key_path, 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
        
        # Convert private key to bytes
        pkb = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        # Connect to Snowflake using RSA key
        conn = snowflake.connector.connect(
            user='ChakoraHub',
            account='gpguymt-ta88699',
            private_key=pkb,
            warehouse='COMPUTE_WH',
            database='"VSRSUBHASH$CHAKORA_DB"',
            schema="CHAKORA"
        )
        print("✅ Connected to Snowflake using RSA key")
        return conn
        
    except Exception as e:
        print("❌ DB Connection Error:", e)
        return None

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    """
    Verifies Basic Auth credentials against the Snowflake database.
    Returns a dict with username and role.
    """
    username = credentials.username
    password = credentials.password
    
    # HARDCODED TEST CREDENTIALS FOR DEVELOPMENT
    # Remove this in production!
    if username == "student" and password == "student":
        print("✅ Using hardcoded test credentials: student/student")
        return {
            "username": "student",
            "role": "student",
            "usertype": "student"
        }
    
    if username == "admin" and password == "admin":
        print("✅ Using hardcoded test credentials: admin/admin")
        return {
            "username": "admin",
            "role": "admin",
            "usertype": "admin"
        }

    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(snowflake.connector.DictCursor)
    try:
        # Fetch user data including type/role - using %(name)s format for Snowflake
        cursor.execute("""
            SELECT u.EMAIL, u.PHONE, u.USERTYPE, l.PASSWORD 
            FROM nrm_users u
            JOIN nrm_logins l ON u.ID = l.USER_ID
            WHERE u.EMAIL = %(username)s OR u.PHONE = %(username)s
            LIMIT 1
        """, {"username": username})
        
        user = cursor.fetchone()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Basic"},
            )

        db_password = user.get('PASSWORD') or ''
        
        # Verify the password
        if db_password.startswith('scrypt:'):
            valid = check_password_hash(db_password, password)
        else:
            valid = (db_password == password)

        if not valid:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Basic"},
            )

        # Determine role from USERTYPE
        usertype = (user.get('USERTYPE') or 'student').lower()
        role = "admin" if usertype == "admin" else "student"

        return {
            "username": username,
            "role": role,
            "usertype": usertype
        }
    except Exception as e:
        print(f"❌ Error in get_current_user: {e}")
        print(f"   Username attempted: {username}")
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"Authentication error: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def is_quick_touchpoint(description: str, complexity: str) -> bool:
    """
    Heuristic to determine if a meeting is a 5-minute quick touchpoint.
    """
    quick_keywords = ["confirm", "status", "quick", "check", "update", "info", "share"]
    desc_lower = description.lower()
    
    # Heuristic Rule 1: User explicitly marked it Easy and description is short
    if complexity == "Easy" and len(description) < 100:
        return True
        
    # Heuristic Rule 2: Contains 'quick' intent keywords
    if any(word in desc_lower for word in quick_keywords) and len(description) < 150:
        return True
        
    return False

# ------------------------------
# MODELS
# ------------------------------
class BookingRequest(BaseModel):
    email: Optional[EmailStr] = None
    date: str
    start_time: str
    duration_minutes: int
    complexity: str = "Medium"

    # ML DATA FIELDS - OPTIONAL (only for internal bookings)
    employee_emails: Optional[List[str]] = None
    purpose: Optional[str] = None
    issue_description: Optional[str] = None

    # New field to distinguish booking type
    booking_type: Optional[str] = "external"  # "external" or "internal"

    # Razorpay payment fields (required for external bookings)
    payment_id: Optional[str] = ""
    order_id: Optional[str] = ""
    signature: Optional[str] = ""

class BookingReasonRequest(BaseModel):
    email: Optional[str] = None
    phone: Optional[str] = None
    reason: str


class BookingPurposeRequest(BaseModel):
    booking_id: str
    purpose: str


class IdentityLookupRequest(BaseModel):
    identity: str

class TeamsTranscriptRequest(BaseModel):
    student_email: str       # Or use EmailStr if you want strict validation
    transcript: str          # The raw transcript text string
    booking_reason: str      # Context regarding the meeting's purpose
    instructor_rating: int   # Rating integer passed from the front-end

class AdminApproveRequest(BaseModel):
    booking_id: str
    action: str = "approve"   # "approve" or "reject"


class CancelResponse(BaseModel):
    message: str


# NEW: request model for initiating a pending hold
class PendingHoldRequest(BaseModel):
    date: str
    start_time: str
    duration_minutes: int
    complexity: str = "Medium"
    booking_type: Optional[str] = "external"


# ── AUTO SUGGESTION MODELS ────────────────────────────────────────────────────
class AutoSuggestionRequest(BaseModel):
    email: Optional[str] = None
    booking_reason: Optional[str] = None   # student's free-text description
    date: Optional[str] = None             # requested date (for demand context)
    start_time: Optional[str] = None       # requested start time

# ==========================================
# COMBINED LOGIN ENDPOINT (FastAPI)
# ==========================================
@app.post('/nrm_logins')
async def user_nrm_logins(request: Request):
    """
    FastAPI-compatible combined login.
    Uses form data (application/x-www-form-urlencoded) and returns RedirectResponse with cookies set.
    """
    form = await request.form()
    login_type = (form.get('login_type') or 'user').strip().lower()
    password = (form.get('password') or '').strip()

    conn = get_db_connection()
    if conn is None:
        return JSONResponse(
            content={"message": "Database connection failed"},
            status_code=500
        )

    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)

        if login_type == 'admin':
            admin_username = form.get('admin_username', '').strip()
            if not admin_username or not password:
                return JSONResponse(
                    content={"message": "Admin username and password are required"},
                    status_code=400
                )

            cursor.execute("""
                SELECT u.ID, u.EMAIL, u.PHONE, u.USERTYPE, u.FULLNAME, u.PROFILE_PICTURE_PATH,
                       l.PASSWORD
                FROM nrm_users u
                JOIN nrm_logins l ON u.ID = l.USER_ID
                WHERE (u.EMAIL = %s OR u.PHONE = %s) AND u.USERTYPE = 'admin'
                LIMIT 1
            """, (admin_username, admin_username))

            admin_row = cursor.fetchone()
            if not admin_row:
                return JSONResponse(
                    content={"message": "Invalid admin credentials"},
                    status_code=401
                )

            db_password = admin_row.get('PASSWORD') or ''
            if db_password.startswith('scrypt:'):
                valid = check_password_hash(db_password, password)
            else:
                valid = (db_password == password)

            if not valid:
                return JSONResponse(
                    content={"message": "Invalid admin credentials"},
                    status_code=401
                )

            response = RedirectResponse(url="/adminpage", status_code=303)
            response.set_cookie(key="logged_in", value="True", httponly=True, secure=True, samesite="Strict")
            response.set_cookie(key="user_id", value=str(admin_row['ID']), httponly=True, secure=True, samesite="Strict")
            response.set_cookie(key="user_type", value="admin", httponly=True, secure=True, samesite="Strict")
            return response

        else:  # user login
            email_or_phone = form.get('email_or_phone', '').strip()
            if not email_or_phone or not password:
                return JSONResponse(
                    content={"message": "Email/Phone and password are required"},
                    status_code=400
                )

            cursor.execute("""
                SELECT u.ID, u.EMAIL, u.PHONE, u.USERTYPE, u.FULLNAME, u.PROFILE_PICTURE_PATH,
                       l.PASSWORD
                FROM nrm_users u
                JOIN nrm_logins l ON u.ID = l.USER_ID
                WHERE u.EMAIL = %s OR u.PHONE = %s
                LIMIT 1
            """, (email_or_phone, email_or_phone))

            user_row = cursor.fetchone()
            if not user_row:
                return JSONResponse(content={"message": "Invalid user credentials"}, status_code=401)

            db_password = user_row.get('PASSWORD') or ''
            if db_password.startswith('scrypt:'):
                valid = check_password_hash(db_password, password)
            else:
                valid = (db_password == password)

            if not valid:
                return JSONResponse(content={"message": "Invalid user credentials"}, status_code=401)

            response = RedirectResponse(url="/aboutus", status_code=303)
            response.set_cookie(key="logged_in", value="True", httponly=True, secure=True, samesite="Strict")
            response.set_cookie(key="user_id", value=str(user_row['ID']), httponly=True, secure=True, samesite="Strict")
            user_type = (user_row.get('USERTYPE') or 'student').lower()
            response.set_cookie(key="user_type", value=user_type, httponly=True, secure=True, samesite="Strict")
            return response

    finally:
        cursor.close()
        conn.close()

# ==========================================
# WHOAMI - Return user info with correct role
# ==========================================
@app.get("/whoami")
def whoami(user=Depends(get_current_user)):
    """
    Returns the authenticated user's information including role.
    Frontend uses this to determine internal vs external user.
    """
    return {
        "username": user["username"],
        "role": user["role"],  # "admin" or "student"
        "authenticated": True
    }


# ==========================================
# HELPER FUNCTIONS
# ==========================================
def to_minutes(t_str: str) -> int:
    """10:30 -> 630"""
    h, m = map(int, t_str.split(":"))
    return h * 60 + m


def from_minutes(mins: int) -> str:
    """630 -> 10:30"""
    h = mins // 60
    m = mins % 60
    return f"{h:02d}:{m:02d}"


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(to_float(value, default))
    except (TypeError, ValueError):
        return default


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def is_valid_email(value: str) -> bool:
    return bool(EMAIL_REGEX.match((value or "").strip()))


def is_org_email(value: Optional[str]) -> bool:
    return str(value or "").strip().lower().endswith("@chakorahub.com")

def get_rag_context_for_suggestion(student_email: str, booking_reason: str) -> str:
    """Fetch past session context from RAG service for suggestion enrichment."""
    try:
        resp = http_requests.post(
            f"{RAG_SERVICE_URL}/rag-context",
            json={"student_email": student_email, "booking_reason": booking_reason or ""},
            timeout=5,
        )
        return resp.json().get("context", "") if resp.ok else ""
    except Exception as exc:
        print(f"⚠️ RAG context fetch failed: {exc}")
        return ""

def scan_all_bookings(filter_expression=None) -> List[dict]:
    scan_kwargs = {}
    if filter_expression is not None:
        scan_kwargs["FilterExpression"] = filter_expression

    response = bookings_table.scan(**scan_kwargs)
    items = response.get("Items", [])

    while "LastEvaluatedKey" in response:
        scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        response = bookings_table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))

    return items


def count_active_bookings(bookings: List[dict]) -> int:
    return sum(1 for booking in bookings if booking.get("status") not in ("CANCELLED", "REJECTED"))


def complexity_score(complexity: str) -> float:
    return COMPLEXITY_MULTIPLIER.get(complexity, COMPLEXITY_MULTIPLIER["Medium"])


def build_feature_vector(
    duration_minutes: int,
    complexity: str,
    demand_score: int,
    lead_time_days: int,
    team_size: int,
    is_internal: bool,
    is_existing: bool,
    start_minutes: int,
) -> List[float]:
    return [
        1.0,
        float(duration_minutes),
        complexity_score(complexity),
        float(max(demand_score, 0)),
        float(max(lead_time_days, 0)),
        float(max(team_size, 1)),
        1.0 if is_internal else 0.0,
        1.0 if is_existing else 0.0,
        float(start_minutes) / 60.0,
    ]


# solve_linear_system removed — replaced by sklearn Ridge


def apply_psychological_rounding(value: float, minimum_price: int) -> int:
    rounded_price = math.ceil(value)
    if 95 <= (rounded_price % 100) <= 105:
        rounded_price = (rounded_price // 100 * 100) - 1
    return max(rounded_price, minimum_price)


def estimate_lead_time_days(item: dict) -> int:
    cached_value = item.get("ml_lead_time")
    if cached_value is not None:
        return max(to_int(cached_value), 0)

    created_at = parse_datetime(item.get("created_at"))
    booking_date = item.get("booking_date")
    if created_at and booking_date:
        try:
            scheduled_date = datetime.fromisoformat(str(booking_date)).date()
            return max((scheduled_date - created_at.date()).days, 0)
        except ValueError:
            return 0
    return 0


def build_training_rows(bookings: List[dict]) -> List[dict]:
    rows = []

    for item in bookings:
        if item.get("status") in ("CANCELLED", "REJECTED"):
            continue

        duration_minutes = to_int(item.get("duration_minutes"))
        observed_price = to_float(item.get("price"))
        if duration_minutes <= 0 or observed_price <= 0:
            continue

        booking_type = (item.get("booking_type") or "external").lower()
        team_size = max(to_int(item.get("team_size"), 1), 1)
        start_minutes = to_int(item.get("start_ts"))
        features = build_feature_vector(
            duration_minutes=duration_minutes,
            complexity=item.get("complexity") or "Medium",
            demand_score=to_int(item.get("ml_demand_score")),
            lead_time_days=estimate_lead_time_days(item),
            team_size=team_size,
            is_internal=(booking_type == "internal"),
            is_existing=bool(item.get("is_existing", False)),
            start_minutes=start_minutes,
        )
        rows.append({"features": features, "price": observed_price})

    return rows


def train_pricing_model(bookings: List[dict]) -> Dict[str, Any]:
    rows = build_training_rows(bookings)
    if len(rows) < MIN_TRAINING_SAMPLES:
        return {
            "version": PRICING_MODEL_VERSION,
            "trained_at": datetime.utcnow().isoformat(),
            "training_samples": len(rows),
            "r2": 0.0,
            "coefficients": [],
            "status": "insufficient_data",
        }

    # NOTE: build_feature_vector prepends a bias term (1.0) as element [0].
    # We drop it here because Ridge handles the intercept internally (fit_intercept=True).
    X = [row["features"][1:] for row in rows]
    y = [row["price"] for row in rows]
    feature_count = len(rows[0]["features"])

    sk_model = Ridge(alpha=RIDGE_REGULARIZATION, fit_intercept=True)
    sk_model.fit(X, y)

    predicted_prices = sk_model.predict(X).tolist()
    r2 = float(r2_score(y, predicted_prices))
    r2 = max(min(r2, 1.0), -1.0)

    # Store intercept + coefficients mirroring the original feature vector layout
    # (features[0] is the bias term) so predict_price_from_model stays compatible.
    coefficients = [round(float(sk_model.intercept_), 6)] + [round(float(c), 6) for c in sk_model.coef_]

    return {
        "version": PRICING_MODEL_VERSION,
        "trained_at": datetime.utcnow().isoformat(),
        "training_samples": len(rows),
        "feature_count": feature_count,
        "r2": round(r2, 4),
        "coefficients": coefficients,
        "status": "trained",
    }


def get_cached_pricing_model() -> Optional[Dict[str, Any]]:
    try:
        cached_model = redis_get_safe(PRICING_MODEL_CACHE_KEY)
        if not cached_model:
            return None
        return json.loads(cached_model)
    except Exception as exc:
        print(f"Pricing model cache read failed: {exc}")
        return None


def cache_pricing_model(model: Dict[str, Any]) -> None:
    try:
        redis_setex_safe(PRICING_MODEL_CACHE_KEY, TTL_PRICING_MODEL, json.dumps(model))
    except Exception as exc:
        print(f"Pricing model cache write failed: {exc}")


def get_or_train_pricing_model(force_retrain: bool = False) -> Dict[str, Any]:
    if not force_retrain:
        cached_model = get_cached_pricing_model()
        if cached_model:
            return cached_model

    model = train_pricing_model(scan_all_bookings())
    cache_pricing_model(model)
    return model


def predict_price_from_model(model: Dict[str, Any], features: List[float]) -> Optional[float]:
    coefficients = model.get("coefficients") or []
    if model.get("status") != "trained" or len(coefficients) != len(features):
        return None
    prediction = sum(weight * value for weight, value in zip(coefficients, features))
    return max(prediction, float(MINIMUM_CHARGE_EXISTING))


def optimize_predicted_price(
    heuristic_price: int,
    model_price: Optional[float],
    model: Dict[str, Any],
) -> Tuple[int, Dict[str, Any]]:
    if model_price is None or model.get("status") != "trained":
        return heuristic_price, {
            "strategy": "heuristic_only",
            "confidence": 0.0,
            "ml_predicted_price": None,
            "training_samples": model.get("training_samples", 0),
            "model_r2": model.get("r2", 0.0),
        }

    training_samples = max(to_int(model.get("training_samples")), 0)
    model_r2 = max(to_float(model.get("r2")), 0.0)
    sample_factor = min(training_samples / 30.0, 1.0)
    confidence = min(0.65, 0.15 + (0.35 * sample_factor) + (0.15 * model_r2))

    lower_bound = heuristic_price * (1 - MAX_MODEL_PRICE_DELTA)
    upper_bound = heuristic_price * (1 + MAX_MODEL_PRICE_DELTA)
    bounded_model_price = min(max(model_price, lower_bound), upper_bound)
    minimum_price = MINIMUM_CHARGE_EXISTING if heuristic_price <= MINIMUM_CHARGE_EXISTING else 200
    optimized_price = apply_psychological_rounding(
        (heuristic_price * (1 - confidence)) + (bounded_model_price * confidence),
        minimum_price=minimum_price,
    )

    return optimized_price, {
        "strategy": "blended_ml_and_heuristic",
        "confidence": round(confidence, 4),
        "ml_predicted_price": round(model_price, 2),
        "bounded_ml_price": round(bounded_model_price, 2),
        "training_samples": training_samples,
        "model_r2": model_r2,
    }


# ══════════════════════════════════════════════════════════════════════════════
# AUTO SUGGESTION ML SYSTEM
# Stack (per deployment diagram):
#   Storage:           DynamoDB  (same bookings_table)
#   Feature Eng:       pandas-style manual (no pandas import needed)
#   Semantic Features: BGE-M3 embeddings via RAG service
#   ML Framework:      scikit-learn
#   ML Models:         Ridge (duration) + LogisticRegression + RandomForest (complexity)
#   Model Storage:     joblib-serialised payload cached in Redis DB 0
#   Inference API:     this file (FastAPI  /meeting/agentic-suggestions)
#   Caching:           Redis DB 0 (pricing model) + DB 8 (per-request embedding cache)
#   Monitoring:        PM2 logs  (existing)
#   Retraining:        /pricing-model/train also retrains suggestion model
# ══════════════════════════════════════════════════════════════════════════════


# ── BGE-M3 embedding helper (via RAG service) ─────────────────────────────────

def get_bge_embedding(text: str) -> Optional[List[float]]:
    """
    Fetch a BGE-M3 embedding vector for free-text from the RAG microservice.
    Returns None when the RAG service is unavailable (graceful degradation).
    """
    if not text:
        return None

    # Check embedding cache in redis_service DB 8
    cache_key = f"{BGE_EMBEDDING_CACHE_PREFIX}{abs(hash(text)) % (10 ** 12)}"
    cached = _rs_get(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass

    try:
        resp = http_requests.post(
            f"{RAG_SERVICE_URL}/embed",
            json={"text": text},
            timeout=5,
        )
        if not resp.ok:
            print(f"⚠️ RAG embed failed: {resp.status_code}")
            return None
        vec = resp.json().get("embedding")
        if isinstance(vec, list) and len(vec) > 0:
            # Cache in redis_service DB 8 for 24 h
            _rs_set(cache_key, json.dumps(vec), BGE_EMBEDDING_CACHE_TTL)
        return vec
    except Exception as exc:
        print(f"⚠️ RAG embed request error: {exc}")
        return None


# ── Feature engineering for auto suggestion ───────────────────────────────────

def build_suggestion_feature_vector(
    duration_minutes: int,
    complexity: str,
    booking_count: int,
    lead_time_days: int,
    is_existing: bool,
    embedding: Optional[List[float]] = None,
) -> List[float]:
    """
    Build a fixed-width feature vector for the suggestion models.

    Structural features (6):
        duration_minutes, complexity_encoded, booking_count, lead_time_days,
        is_existing (0/1), hour_bucket (0–8 mapped from lead_time bucket)

    Semantic features (first 32 dims of BGE-M3 embedding, zero-padded if absent):
        Reduced to first 32 dims to keep the vector lightweight.
        If embedding is None (RAG unavailable), all 32 dims = 0.0.
    """
    try:
        complexity_enc = float(COMPLEXITY_ENCODER.transform([complexity])[0])
    except Exception:
        complexity_enc = 1.0  # default to "Medium" index

    structural = [
        float(duration_minutes),
        complexity_enc,
        float(max(booking_count, 0)),
        float(max(lead_time_days, 0)),
        1.0 if is_existing else 0.0,
        min(float(lead_time_days) / 7.0, 4.0),   # week-bucket capped at 4
    ]

    # Semantic component — first 32 dims of BGE-M3
    EMBED_DIMS = 32
    if embedding and len(embedding) >= EMBED_DIMS:
        semantic = [float(v) for v in embedding[:EMBED_DIMS]]
    else:
        semantic = [0.0] * EMBED_DIMS

    return structural + semantic


def build_suggestion_training_rows(bookings: List[dict]) -> List[dict]:
    """
    Extract labelled rows from historical DynamoDB bookings for suggestion training.
    Each row becomes a training sample for both sub-models:
        - duration_label  → Ridge regression target
        - complexity_label → LogisticRegression + RandomForest target
    """
    rows = []
    for item in bookings:
        if item.get("status") in ("CANCELLED", "REJECTED"):
            continue

        duration_minutes = to_int(item.get("duration_minutes"))
        complexity = (item.get("complexity") or "Medium")
        if complexity not in COMPLEXITY_LABELS:
            complexity = "Medium"
        if duration_minutes <= 0:
            continue

        booking_count = to_int(item.get("ml_demand_score"))
        lead_time = estimate_lead_time_days(item)
        is_existing = bool(item.get("is_existing", False))

        # No embedding during training (would be too slow to re-fetch all)
        features = build_suggestion_feature_vector(
            duration_minutes=duration_minutes,
            complexity=complexity,
            booking_count=booking_count,
            lead_time_days=lead_time,
            is_existing=is_existing,
            embedding=None,
        )

        try:
            complexity_label = int(COMPLEXITY_ENCODER.transform([complexity])[0])
        except Exception:
            complexity_label = 1

        rows.append({
            "features": features,
            "duration_label": float(duration_minutes),
            "complexity_label": complexity_label,
        })

    return rows


# ── Model training ─────────────────────────────────────────────────────────────

def train_auto_suggestion_model(bookings: List[dict]) -> Dict[str, Any]:
    """
    Train three models on historical booking data:
        1. Ridge Regression    → suggest duration_minutes
        2. Logistic Regression → suggest complexity (multi-class)
        3. Random Forest       → suggest complexity (ensemble, for confidence)

    Returns a serialisable dict stored in Redis DB 0.
    """
    rows = build_suggestion_training_rows(bookings)

    if len(rows) < AUTO_SUGGESTION_MIN_SAMPLES:
        return {
            "version": AUTO_SUGGESTION_MODEL_VERSION,
            "trained_at": datetime.utcnow().isoformat(),
            "training_samples": len(rows),
            "status": "insufficient_data",
            "ridge_coef": [],
            "ridge_intercept": 0.0,
            "lr_coef": [],
            "lr_intercept": [],
            "lr_classes": [],
            "rf_estimators": None,   # can't serialise RF simply — store feature importances only
            "rf_feature_importances": [],
            "rf_classes": [],
            "duration_r2": 0.0,
            "complexity_lr_accuracy": 0.0,
            "complexity_rf_accuracy": 0.0,
            "median_duration": 30,
        }

    X = [row["features"] for row in rows]
    y_dur = [row["duration_label"] for row in rows]
    y_cplx = [row["complexity_label"] for row in rows]

    median_duration = float(np.median(y_dur))

    # 1. Ridge — duration prediction
    ridge = Ridge(alpha=RIDGE_REGULARIZATION, fit_intercept=True)
    ridge.fit(X, y_dur)
    duration_r2 = float(r2_score(y_dur, ridge.predict(X)))

    # 2. Logistic Regression — complexity classification
    lr = LogisticRegression(max_iter=500, C=1.0, solver="lbfgs")
    lr.fit(X, y_cplx)
    lr_accuracy = float(accuracy_score(y_cplx, lr.predict(X)))

    # 3. Random Forest — complexity classification (diversity + feature importance)
    rf = RandomForestClassifier(n_estimators=50, max_depth=6, random_state=42)
    rf.fit(X, y_cplx)
    rf_accuracy = float(accuracy_score(y_cplx, rf.predict(X)))

    print(
        f"✅ Auto suggestion model trained | samples={len(rows)} "
        f"duration_r2={duration_r2:.3f} lr_acc={lr_accuracy:.3f} rf_acc={rf_accuracy:.3f}"
    )

    return {
        "version": AUTO_SUGGESTION_MODEL_VERSION,
        "trained_at": datetime.utcnow().isoformat(),
        "training_samples": len(rows),
        "status": "trained",
        # Ridge
        "ridge_coef": [round(float(c), 6) for c in ridge.coef_],
        "ridge_intercept": round(float(ridge.intercept_), 6),
        # Logistic Regression
        "lr_coef": [[round(float(v), 6) for v in row] for row in lr.coef_],
        "lr_intercept": [round(float(v), 6) for v in lr.intercept_],
        "lr_classes": [int(c) for c in lr.classes_],
        # Random Forest — store feature importances & class list (not estimators)
        "rf_feature_importances": [round(float(v), 6) for v in rf.feature_importances_],
        "rf_classes": [int(c) for c in rf.classes_],
        "rf_accuracy": round(rf_accuracy, 4),
        # Metrics
        "duration_r2": round(duration_r2, 4),
        "complexity_lr_accuracy": round(lr_accuracy, 4),
        "complexity_rf_accuracy": round(rf_accuracy, 4),
        "median_duration": round(median_duration, 1),
    }


# NOTE: Random Forest cannot be reconstructed from importances alone.
# For inference we re-use LogisticRegression (fully serialisable via coef/intercept)
# and use RF accuracy as a confidence signal.  If you need full RF inference, use
# joblib.dump() to /tmp and load it back — outside scope of this in-memory cache.


def _lr_predict_proba(model_data: Dict, features: List[float]) -> Dict[int, float]:
    """
    Manual softmax inference using the stored LR coefficients.
    Returns {class_index: probability}.
    """
    coef = model_data.get("lr_coef", [])
    intercept = model_data.get("lr_intercept", [])
    classes = model_data.get("lr_classes", [])

    print(f"🎯 LR classification | coef_count={len(coef)} intercept_count={len(intercept)} classes={classes}")
    if not coef or not classes:
        print(f"❌ LR missing coef or classes")
        return {}

    X = np.array(features, dtype=float)
    scores = []
    for i, row in enumerate(coef):
        w = np.array(row, dtype=float)
        b = float(intercept[i]) if i < len(intercept) else 0.0
        scores.append(float(np.dot(w, X) + b))

    # Softmax
    scores_arr = np.array(scores)
    scores_arr -= scores_arr.max()   # numerical stability
    exp_scores = np.exp(scores_arr)
    probs = exp_scores / exp_scores.sum()
    result_dict = {int(cls): float(p) for cls, p in zip(classes, probs)}
    print(f"✅ LR probabilities: {result_dict}")
    return result_dict


def _ridge_predict_duration(model_data: Dict, features: List[float]) -> Optional[float]:
    coef = model_data.get("ridge_coef", [])
    intercept = model_data.get("ridge_intercept", 0.0)
    print(f"🎯 Ridge prediction | coef_len={len(coef)} feature_len={len(features)} intercept={intercept}")
    if not coef or len(coef) != len(features):
        print(f"❌ Ridge mismatch | coef_len={len(coef)} != feature_len={len(features)}")
        return None
    result = float(np.dot(np.array(coef), np.array(features)) + intercept)
    print(f"✅ Ridge duration prediction: {result:.1f} min")
    return result


# ── Auto suggestion model cache (Redis DB 0 — same pool as pricing model) ─────

def get_cached_auto_suggestion_model() -> Optional[Dict[str, Any]]:
    try:
        raw = redis_get_safe(AUTO_SUGGESTION_CACHE_KEY)
        if not raw:
            return None
        return json.loads(raw)
    except Exception as exc:
        print(f"⚠️ Auto suggestion cache read failed: {exc}")
        return None


def cache_auto_suggestion_model(model: Dict[str, Any]) -> None:
    try:
        redis_setex_safe(AUTO_SUGGESTION_CACHE_KEY, AUTO_SUGGESTION_CACHE_TTL, json.dumps(model))
    except Exception as exc:
        print(f"⚠️ Auto suggestion cache write failed: {exc}")


def get_or_train_auto_suggestion_model(force_retrain: bool = False) -> Dict[str, Any]:
    if not force_retrain:
        cached = get_cached_auto_suggestion_model()
        if cached:
            return cached
    model = train_auto_suggestion_model(scan_all_bookings())
    cache_auto_suggestion_model(model)
    return model


# ── Core inference function ────────────────────────────────────────────────────

def infer_auto_suggestions(
    model_data: Dict[str, Any],
    booking_reason: Optional[str],
    current_day_demand: int,
    lead_time_days: int,
    is_existing: bool,
    rag_context=None,
) -> Dict[str, Any]:
    """
    Run the three-model ensemble and return suggestions:
        suggested_duration_minutes : int   (Ridge)
        suggested_complexity       : str   (LogisticRegression + RF-confidence blend)
        complexity_confidence      : float
        semantic_used              : bool  (whether BGE embedding contributed)
    """
    print(f"🚀 INFER AUTO SUGGESTIONS START | model_status={model_data.get('status')} reason_len={len(booking_reason) if booking_reason else 0} demand={current_day_demand} lead_time={lead_time_days} existing={is_existing}")
    if model_data.get("status") != "trained":
        print(f"❌ Model not trained | status={model_data.get('status')} | returning nulls")
        return {
            "suggested_duration_minutes": None,
            "suggested_complexity": None,
            "complexity_confidence": 0.0,
            "semantic_used": False,
            "model_status": model_data.get("status", "unavailable"),
        }

    # Fetch embedding for the booking reason (semantic features)
    enriched_text = " ".join(filter(None, [booking_reason, rag_context])).strip()
    print(f"📝 Enriched text for embedding | len={len(enriched_text)} | has_reason={booking_reason is not None} has_rag={bool(rag_context)}")
    if enriched_text:
        print(f"📝 Enriched text preview: {enriched_text[:100]}...")
    embedding = get_bge_embedding(enriched_text) if enriched_text else None
    semantic_used = embedding is not None
    print(f"🔍 Semantic embedding status | used={semantic_used} | has_embedding={embedding is not None}")

    # Use median duration as seed for feature vector (we're suggesting duration, not given it)
    seed_duration = int(model_data.get("median_duration", 30))
    features = build_suggestion_feature_vector(
        duration_minutes=seed_duration,
        complexity="Medium",         # neutral seed for the suggestion vector
        booking_count=current_day_demand,
        lead_time_days=lead_time_days,
        is_existing=is_existing,
        embedding=embedding,
    )

    # ── Duration suggestion (Ridge) ───────────────────────────────────────────
    raw_duration = _ridge_predict_duration(model_data, features)
    median_dur = float(model_data.get("median_duration", 30))
    if raw_duration is not None:
        # Clamp to ±30% of median and snap to nearest 15-min grid
        min_dur = max(15, median_dur * (1 - DURATION_MAX_DELTA_FACTOR))
        max_dur = median_dur * (1 + DURATION_MAX_DELTA_FACTOR)
        clamped = min(max(raw_duration, min_dur), max_dur)
        suggested_duration = int(round(clamped / 15.0) * 15)
        suggested_duration = max(15, suggested_duration)
    else:
        suggested_duration = None

    # ── Complexity suggestion (LR + RF confidence) ────────────────────────────
    proba_map = _lr_predict_proba(model_data, features)
    if proba_map:
        best_class_idx = max(proba_map, key=lambda k: proba_map[k])
        best_prob = proba_map[best_class_idx]
        try:
            suggested_complexity = str(COMPLEXITY_ENCODER.inverse_transform([best_class_idx])[0])
        except Exception:
            suggested_complexity = "Medium"

        # Blend LR confidence with RF accuracy signal
        rf_accuracy = float(model_data.get("rf_accuracy", model_data.get("complexity_rf_accuracy", 0.5)))
        blended_confidence = (best_prob * 0.6) + (rf_accuracy * 0.4)

        if blended_confidence < COMPLEXITY_MIN_CONFIDENCE:
            suggested_complexity = None
            blended_confidence = 0.0
    else:
        suggested_complexity = None
        blended_confidence = 0.0

    return {
        "suggested_duration_minutes": suggested_duration,
        "suggested_complexity": suggested_complexity,
        "complexity_confidence": round(blended_confidence, 4),
        "semantic_used": semantic_used,
        "model_status": "trained",
        "training_samples": model_data.get("training_samples", 0),
        "duration_r2": model_data.get("duration_r2", 0.0),
        "lr_accuracy": model_data.get("complexity_lr_accuracy", 0.0),
        "rf_accuracy": model_data.get("complexity_rf_accuracy", 0.0),
    }


# ── RAG / S3 helpers (used by /teams-transcript and /submit-booking-reason) ───

def save_reason_to_s3(payload: "BookingReasonRequest") -> None:
    """Persist booking reason to S3 via RAG service."""
    try:
        http_requests.post(
            f"{RAG_SERVICE_URL}/store/reason",
            json={
                "email": payload.email or "",
                "phone": payload.phone or "",
                "reason": payload.reason,
            },
            timeout=8,
        )
    except Exception as exc:
        print(f"⚠️ save_reason_to_s3 failed: {exc}")


def save_transcript_to_s3(payload: "TeamsTranscriptRequest") -> None:
    """Persist raw Teams transcript to S3 via RAG service."""
    try:
        http_requests.post(
            f"{RAG_SERVICE_URL}/store/transcript",
            json={
                "student_email": payload.student_email,
                "transcript": payload.transcript,
                "booking_reason": payload.booking_reason,
                "instructor_rating": payload.instructor_rating,
            },
            timeout=10,
        )
    except Exception as exc:
        print(f"⚠️ save_transcript_to_s3 failed: {exc}")


def ingest_transcript(
    student_email: str,
    transcript_text: str,
    booking_reason: str,
    instructor_rating: int,
) -> dict:
    """Trigger RAG ingestion pipeline for a transcript."""
    try:
        resp = http_requests.post(
            f"{RAG_SERVICE_URL}/ingest",
            json={
                "student_email": student_email,
                "transcript": transcript_text,
                "booking_reason": booking_reason,
                "instructor_rating": instructor_rating,
            },
            timeout=30,
        )
        return resp.json() if resp.ok else {"success": False, "message": resp.text}
    except Exception as exc:
        print(f"⚠️ RAG ingest failed: {exc}")
        return {"success": False, "message": str(exc)}


def analyze_student_progress(student_email: str) -> dict:
    """Pull progress analysis from student service."""
    try:
        resp = http_requests.get(
            f"{STUDENT_SERVICE_URL}/progress",
            params={"email": student_email},
            timeout=10,
        )
        return resp.json() if resp.ok else {"error": resp.text}
    except Exception as exc:
        print(f"⚠️ student progress fetch failed: {exc}")
        return {"error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
def lead_time_factor(lead_time_days: int) -> float:
    """Graduated lead-time incentive/penalty for clearer pricing differentiation."""
    if lead_time_days >= 21:
        return 0.80
    if lead_time_days >= 14:
        return 0.85
    if lead_time_days >= 7:
        return EARLY_BIRD_DISCOUNT
    if lead_time_days <= 1:
        return 1.10
    return 1.0


def slot_time_factor(start_minutes: int) -> float:
    """Peak slots cost more, early/late slots get a small discount."""
    start_hour = start_minutes / 60.0
    if 11.0 <= start_hour <= 13.0 or 17.0 <= start_hour <= 18.5:
        return PEAK_HOUR_MULTIPLIER
    if start_hour < 11.0 or start_hour > 18.0:
        return OFFPEAK_HOUR_DISCOUNT
    return 1.0


def calculate_dynamic_price(
    is_existing: bool,
    duration_minutes: int,
    complexity: str,
    day_bookings_count: int,
    lead_time_days: int,
    start_minutes: int,
    is_internal: bool = False,
) -> Tuple[int, Dict[str, float]]:
    """Phase 2 heuristic pricing with transparent factor breakdown."""
    if is_internal:
        return INTERNAL_DOMAIN_PRICE, {
            "base_calc": float(INTERNAL_DOMAIN_PRICE),
            "demand_factor": 1.0,
            "supply_factor": 0.0,
            "lead_time_factor": 1.0,
            "slot_time_factor": 1.0,
            "final_price_raw": float(INTERNAL_DOMAIN_PRICE),
        }

    hours = duration_minutes / 60.0
    multiplier = COMPLEXITY_MULTIPLIER.get(complexity, 1.0)
    base_calc = hours * BASE_RATE * multiplier

    demand_factor = min(1 + (day_bookings_count * DEMAND_MULTIPLIER), MAX_DEMAND_FACTOR)

    supply_factor = 0.0
    if day_bookings_count > (TOTAL_STAFF * 0.7):
        supply_factor = float(SUPPLY_PENALTY)

    lead_factor = lead_time_factor(lead_time_days)
    time_factor = slot_time_factor(start_minutes)

    final_price_raw = (base_calc * demand_factor * lead_factor * time_factor) + supply_factor

    if lead_time_days >= 21:
        final_price_raw = min(final_price_raw, base_calc * 0.90)
    elif lead_time_days >= 14:
        final_price_raw = min(final_price_raw, base_calc * 0.95)

    final_price = apply_psychological_rounding(final_price_raw, minimum_price=200)

    return final_price, {
        "base_calc": round(base_calc, 2),
        "demand_factor": round(demand_factor, 4),
        "supply_factor": round(supply_factor, 2),
        "lead_time_factor": round(lead_factor, 4),
        "slot_time_factor": round(time_factor, 4),
        "final_price_raw": round(final_price_raw, 2),
    }


def send_booking_email(
    to_email: str,
    details: dict,
    is_admin: bool = False,
    cc_admin: bool = False,
    action_type: str = "new_booking",
):
    """
    Send booking confirmation/notification email — rich HTML template.
    Theme: teal (#00897b) card-style, matching ChakoraHub app UI.
    """
    try:
        # ── Unpack details ────────────────────────────────────────────────
        booking_type = details.get("booking_type", "external")
        is_internal  = (booking_type == "internal")
        booking_id   = details.get("booking_id",       "N/A")
        student_name = details.get("student_name",     "Student")
        date_str     = details.get("date",             "N/A")
        start_time   = details.get("start_time",       "N/A")
        duration_min = details.get("duration_minutes", "N/A")
        price        = details.get("price",            "N/A")
        complexity   = details.get("complexity",       "N/A")
        status       = details.get("status",           "PENDING")
        action_by    = details.get("action_by",        "Admin")
        meeting_link = details.get("meeting_link",     "")

        # ── Brand tokens ──────────────────────────────────────────────────
        TEAL        = "#00897b"
        TEAL_DARK   = "#00695c"
        TEAL_LIGHT  = "#e0f2f1"
        SUCCESS     = "#2e7d32"
        SUCCESS_BG  = "#e8f5e9"
        SUCCESS_BD  = "#4caf50"
        DANGER      = "#c62828"
        DANGER_BG   = "#ffebee"
        DANGER_BD   = "#ef5350"
        WARN        = "#e65100"
        WARN_BG     = "#fff3e0"
        WARN_BD     = "#ff9800"
        GRAY_BG     = "#f5f5f5"
        BORDER      = "#e0e0e0"

        # ── Status config ─────────────────────────────────────────────────
        STATUS_CFG = {
            "PENDING":  {"icon": "&#9203;",  "label": "Pending Review", "color": WARN,    "bg": WARN_BG,    "border": WARN_BD},
            "APPROVED": {"icon": "&#10004;", "label": "Approved",       "color": SUCCESS, "bg": SUCCESS_BG, "border": SUCCESS_BD},
            "REJECTED": {"icon": "&#10008;", "label": "Rejected",       "color": DANGER,  "bg": DANGER_BG,  "border": DANGER_BD},
        }
        sc = STATUS_CFG.get(status, STATUS_CFG["PENDING"])

        # ── Reusable HTML snippets ────────────────────────────────────────
        def row(label, value, last=False):
            bb = "" if last else f"border-bottom:1px solid {BORDER};"
            return (
                f'<tr>'
                f'<td style="width:38%;padding:11px 14px;background:{GRAY_BG};'
                f'font-weight:600;font-size:13px;color:#424242;{bb}">{label}</td>'
                f'<td style="width:62%;padding:11px 14px;background:#fff;'
                f'font-size:13px;color:#212121;{bb}">{value}</td>'
                f'</tr>'
            )

        def section_header(text):
            return (
                f'<tr><th colspan="2" style="background:{TEAL};color:#fff;'
                f'padding:10px 14px;text-align:left;font-size:13px;'
                f'font-weight:600;letter-spacing:0.3px;">{text}</th></tr>'
            )

        def table_wrap(inner_rows):
            return (
                f'<table style="width:100%;border-collapse:collapse;'
                f'border:1px solid {BORDER};border-radius:4px;overflow:hidden;'
                f'margin-bottom:20px;">{inner_rows}</table>'
            )

        def status_badge():
            return (
                f'<div style="display:inline-block;background:{sc["bg"]};'
                f'border:1px solid {sc["border"]};border-radius:20px;'
                f'padding:6px 16px;font-size:13px;font-weight:700;'
                f'color:{sc["color"]};margin-bottom:18px;">'
                f'{sc["icon"]} &nbsp;{sc["label"]}'
                f'</div>'
            )

        def divider():
            return f'<hr style="border:none;border-top:1px solid {BORDER};margin:20px 0;">'

        def tip_box(header, items):
            li_html = "".join(
                f'<li style="margin-bottom:7px;font-size:13px;">{i}</li>'
                for i in items
            )
            return (
                f'<div style="background:{TEAL_LIGHT};border-left:4px solid {TEAL};'
                f'border-radius:4px;padding:14px 16px;margin-top:20px;">'
                f'<p style="margin:0 0 8px;font-weight:700;color:{TEAL_DARK};font-size:13px;">'
                f'&#128204; {header}</p>'
                f'<ul style="margin:0;padding-left:18px;">{li_html}</ul>'
                f'</div>'
            )

        def header_block(title, subtitle=""):
            sub = (
                f'<p style="margin:4px 0 0;font-size:13px;color:#ccf2ee;">{subtitle}</p>'
                if subtitle else ""
            )
            return (
                f'<div style="background-color:{TEAL};color:#ffffff;'
                f'padding:28px 24px;text-align:center;">'
                f'<div style="font-size:28px;margin-bottom:8px;">&#128197;</div>'
                f'<h2 style="margin:0;font-size:22px;font-weight:700;'
                f'letter-spacing:0.5px;color:#ffffff;">'
                f'{title}</h2>{sub}'
                f'</div>'
            )

        def footer_block():
            return (
                f'<div style="background:{GRAY_BG};padding:18px 24px;'
                f'text-align:center;border-top:1px solid {BORDER};">'
                f'<p style="margin:0 0 4px;font-size:13px;color:#424242;">'
                f'Regards, <strong>ChakoraHub Team</strong></p>'
                f'<p style="margin:0;font-size:11px;color:#9e9e9e;">'
                f'This is an automated message &mdash; please do not reply.</p>'
                f'</div>'
            )

        def booking_id_chip():
            return (
                f'<div style="background:{GRAY_BG};border:1px solid {BORDER};'
                f'border-radius:4px;padding:10px 14px;margin-bottom:18px;'
                f'font-size:12px;color:#616161;">'
                f'<strong style="color:{TEAL};">Booking ID</strong>&nbsp;&nbsp;'
                f'<span style="font-family:monospace;font-size:12px;">{booking_id}</span>'
                f'</div>'
            )

        def teams_join_button(url):
            """Renders a prominent Teams join button. Returns empty string if no URL."""
            if not url:
                return ""
            return (
                f'<div style="text-align:center;margin:24px 0;">'
                f'<a href="{url}" target="_blank" '
                f'style="display:inline-block;background:{TEAL};color:#ffffff;'
                f'text-decoration:none;padding:14px 32px;border-radius:6px;'
                f'font-size:15px;font-weight:700;letter-spacing:0.3px;">'
                f'&#128222; Join Teams Meeting</a>'
                f'</div>'
                f'<p style="text-align:center;font-size:11px;color:#9e9e9e;margin-top:-12px;">'
                f'Or copy the link: '
                f'<a href="{url}" style="color:{TEAL};font-size:11px;word-break:break-all;">'
                f'{url[:80]}{"..." if len(url) > 80 else ""}</a></p>'
            )

        def wrap(content):
            return (
                '<!DOCTYPE html>'
                '<html xmlns="http://www.w3.org/1999/xhtml">'
                '<head><meta charset="UTF-8">'
                '<meta name="viewport" content="width=device-width,initial-scale=1">'
                '</head>'
                '<body style="margin:0;padding:0;background-color:#eeeeee;'
                'font-family:Arial,Helvetica,sans-serif;">'
                '<table width="100%" cellpadding="0" cellspacing="0" border="0"'
                ' style="background-color:#eeeeee;padding:20px 0;">'
                '<tr><td align="center">'
                '<table width="580" cellpadding="0" cellspacing="0" border="0"'
                ' style="background-color:#ffffff;border:1px solid #e0e0e0;'
                'max-width:580px;width:100%;">'
                '<tr><td>'
                f'{content}'
                '</td></tr>'
                '</table>'
                '</td></tr></table>'
                '</body></html>'
            )

        # ══════════════════════════════════════════════════════════════════
        # ADMIN EMAIL
        # ══════════════════════════════════════════════════════════════════
        if is_admin:

            # ── NEW BOOKING ───────────────────────────────────────────────
            if action_type == "new_booking":
                subject = f"New Booking Request - {booking_id}"

                internal_block = ""
                if is_internal:
                    internal_block = (
                        table_wrap(
                            section_header("&#128101; Internal Participants")
                            + row("Organizer",   details.get("organizer_email", "N/A"))
                            + row("Attendees",   details.get("employee_emails", "N/A"))
                            + row("Team Size",   str(details.get("team_size", "N/A")))
                            + row("Purpose",     details.get("purpose", "N/A"))
                            + row("Description", details.get("issue_description", "N/A"), last=True)
                        )
                    )

                body_html = wrap(
                    header_block("New Booking Request", "ChakoraHub &mdash; Meeting Management")
                    + f'<div style="padding:24px;">'
                    + f'<p style="margin:0 0 16px;font-size:14px;color:#424242;">'
                    + f'A new meeting has been booked and is awaiting your review.</p>'
                    + booking_id_chip()
                    + status_badge()
                    + table_wrap(
                        section_header("&#128203; Session Details")
                        + row("Student / User", student_name)
                        + row("Date",           date_str)
                        + row("Start Time",     start_time)
                        + row("Duration",       f"{duration_min} minutes")
                        + row("Complexity",     complexity)
                        + row("Booking Type",   "Internal (Employee)" if is_internal else "External (Student)")
                        + row("Price",          f"&#8377;{price}", last=True)
                    )
                    + internal_block
                    + tip_box("Action Required", [
                        "Review the session details above carefully.",
                        f'<a href="{ADMIN_PANEL_URL}" style="color:{TEAL};font-weight:600;">'
                        f'Go to Admin Panel to Approve / Reject &rarr;</a>',
                    ])
                    + footer_block()
                    + '</div>'
                )

                body_text = (
                    f"New Booking Request - {booking_id}\n\n"
                    f"Student  : {student_name}\n"
                    f"Date     : {date_str}\n"
                    f"Time     : {start_time}\n"
                    f"Duration : {duration_min} min\n"
                    f"Price    : Rs.{price}\n"
                    f"Type     : {'Internal' if is_internal else 'External'}\n\n"
                    f"Review at: {ADMIN_PANEL_URL}"
                )

            # ── ACTION TAKEN ──────────────────────────────────────────────
            else:
                action_word = "Approved" if status == "APPROVED" else "Rejected"
                subject = f"Booking {action_word} - {booking_id}"

                meeting_block = ""
                if status == "APPROVED" and meeting_link:
                    meeting_block = (
                        f'<div style="background:{TEAL_LIGHT};border-left:4px solid {TEAL};'
                        f'border-radius:4px;padding:14px 16px;margin-top:16px;">'
                        f'<p style="margin:0 0 6px;font-weight:700;color:{TEAL_DARK};font-size:13px;">'
                        f'&#128279; Teams Meeting Created</p>'
                        f'<p style="margin:0;font-size:12px;color:#424242;word-break:break-all;">'
                        f'<a href="{meeting_link}" style="color:{TEAL};">{meeting_link}</a></p>'
                        f'</div>'
                    )

                body_html = wrap(
                    header_block(f"Booking {action_word}", "ChakoraHub &mdash; Admin Action Summary")
                    + f'<div style="padding:24px;">'
                    + f'<p style="margin:0 0 16px;font-size:14px;color:#424242;">'
                    + f'The booking below has been <strong>{action_word.lower()}</strong> '
                    + f'by <strong>{action_by}</strong>.</p>'
                    + booking_id_chip()
                    + status_badge()
                    + table_wrap(
                        section_header("&#128203; Booking Summary")
                        + row("Student",   student_name)
                        + row("Date",      date_str)
                        + row("Time",      start_time)
                        + row("Duration",  f"{duration_min} minutes")
                        + row("Action By", action_by)
                        + row("Status",    sc["label"], last=True)
                    )
                    + meeting_block
                    + footer_block()
                    + '</div>'
                )

                body_text = (
                    f"Booking {action_word} - {booking_id}\n\n"
                    f"Student   : {student_name}\n"
                    f"Date      : {date_str}\n"
                    f"Time      : {start_time}\n"
                    f"Action by : {action_by}\n"
                    f"Status    : {status}\n"
                    + (f"Teams URL : {meeting_link}\n" if meeting_link else "")
                )

            ses.send_email(
                Source=ADMIN_EMAIL,
                Destination={"ToAddresses": [ADMIN_EMAIL]},
                Message={
                    "Subject": {"Data": subject, "Charset": CHARSET},
                    "Body": {
                        "Html": {"Data": body_html, "Charset": CHARSET},
                        "Text": {"Data": body_text, "Charset": CHARSET},
                    },
                },
            )

        # ══════════════════════════════════════════════════════════════════
        # STUDENT EMAIL
        # ══════════════════════════════════════════════════════════════════
        else:
            if status == "PENDING":
                subject      = f"Booking Received - {booking_id}"
                headline     = "Booking Received!"
                sub_headline = "Your session request is under review."
                greeting     = (
                    "Your meeting booking has been received and is "
                    "<strong>awaiting approval</strong>. We will notify you shortly."
                )
                tips_header  = "What Happens Next?"
                tip_items    = [
                    "Our team will review your request and confirm availability.",
                    "You will receive an approval email within the day.",
                    "Please be available at the selected date &amp; time.",
                ]
                extra_block  = ""

            elif status == "APPROVED":
                subject      = f"Booking Approved - {booking_id}"
                headline     = "Booking Approved!"
                sub_headline = "Your session is confirmed."
                greeting     = (
                    "Great news! Your meeting booking has been <strong>approved</strong>. "
                    "We look forward to seeing you!"
                )
                tips_header  = "Before Your Session"
                tip_items    = [
                    "Please join on time &mdash; be ready 5 minutes early.",
                    "Keep your Booking ID handy for reference.",
                    "Contact support if you need to reschedule.",
                ]
                # Teams join button (only when a link is available)
                extra_block  = teams_join_button(meeting_link)

            else:  # REJECTED
                subject      = f"Booking Update - {booking_id}"
                headline     = "Booking Not Confirmed"
                sub_headline = "We could not approve your request."
                greeting     = (
                    "We regret to inform you that your booking request has been "
                    "<strong>declined</strong>. Please see the details below."
                )
                tips_header  = "Next Steps"
                tip_items    = [
                    "You may submit a new booking for a different time slot.",
                    "Contact our support team if you need clarification.",
                    f'Visit <a href="https://www.chakorahub.com" style="color:{TEAL};">'
                    f'chakorahub.com</a> to book again.',
                ]
                extra_block  = ""

            body_html = wrap(
                header_block(headline, sub_headline)
                + f'<div style="padding:24px;">'
                + f'<p style="margin:0 0 16px;font-size:14px;color:#424242;">'
                + f'Dear <strong>{student_name}</strong>,</p>'
                + f'<p style="margin:0 0 18px;font-size:14px;color:#424242;">{greeting}</p>'
                + booking_id_chip()
                + status_badge()
                + table_wrap(
                    section_header("&#128197; Session Details")
                    + row("Date",         date_str)
                    + row("Start Time",   start_time)
                    + row("Duration",     f"{duration_min} minutes")
                    + row("Complexity",   complexity)
                    + row("Amount",       f"&#8377;{price}", last=True)
                )
                + extra_block
                + tip_box(tips_header, tip_items)
                + footer_block()
                + '</div>'
            )

            body_text = (
                f"Dear {student_name},\n\n"
                f"Booking Status : {status}\n"
                f"Booking ID     : {booking_id}\n"
                f"Date           : {date_str}\n"
                f"Time           : {start_time}\n"
                f"Duration       : {duration_min} minutes\n"
                f"Complexity     : {complexity}\n"
                f"Amount         : Rs.{price}\n"
                + (f"Teams Meeting  : {meeting_link}\n" if meeting_link else "")
                + f"\nThank you,\nChakoraHub Team"
            )

            destination = {"ToAddresses": [to_email]}
            if cc_admin:
                destination["CcAddresses"] = [ADMIN_EMAIL]

            ses.send_email(
                Source=ADMIN_EMAIL,
                Destination=destination,
                Message={
                    "Subject": {"Data": subject, "Charset": CHARSET},
                    "Body": {
                        "Html": {"Data": body_html, "Charset": CHARSET},
                        "Text": {"Data": body_text, "Charset": CHARSET},
                    },
                },
            )

    except Exception as e:
        print(f"Email send error: {e}")


# ==========================================
# MEETING ENDPOINTS
# ==========================================

@app.get("/meeting-purposes")
def get_meeting_purposes():
    """Return list of purposes - CACHED in Redis for 24 hours"""
    cache_key = "meeting_purposes"
    
    cached_data = redis_get_safe(cache_key)
    if cached_data:
        print("🚀 Redis Cache Hit: meeting-purposes")
        return {"purposes": json.loads(cached_data)}
    
    print("❄️ Cache Miss: Returning static list and updating Redis")
    purposes = VALID_PURPOSES
    redis_setex_safe(cache_key, TTL_PURPOSES, json.dumps(purposes))

    return {"purposes": purposes}


@app.post("/meeting/identify")
@app.post("/meeting/api/identify")
def meeting_identify(req: IdentityLookupRequest):
    identity = (req.identity or "").strip().lower()
    if not identity:
        raise HTTPException(status_code=400, detail="identity is required")

    # Cache-aside: Redis first (student:profile), DynamoDB on cache miss.
    profile_key = f"student:profile:{identity}"
    try:
        r = http_requests.get(
            f"{REDIS_SERVICE_URL}/redis/get",
            params={"key": profile_key, "db": 1},
            timeout=3,
        )
        data = r.json() if r.ok else {}
        if data.get("success") and data.get("found"):
            cached = json.loads(data.get("value") or "{}")
            return {
                "success": True,
                "exists": bool(cached.get("exists", False)),
                "total_bookings": int(cached.get("total_bookings", 0) or 0),
                "cache_hit": True,
            }
    except Exception as exc:
        print(f"⚠️ student:profile cache read failed | identity={identity} | error={exc}")

    # Cache miss → DynamoDB fallback
    bookings = scan_all_bookings(
        filter_expression=(
            boto3.dynamodb.conditions.Attr("created_by").eq(identity)
            | boto3.dynamodb.conditions.Attr("student_email").eq(identity)
        )
    )
    total_bookings = count_active_bookings(bookings)

    # Cache-aside write-back (TTL 30 min) on positive hit.
    if total_bookings > 0:
        now_iso = datetime.utcnow().isoformat()
        profile_payload = {
            "identity": identity,
            "exists": True,
            "total_bookings": total_bookings,
            "last_seen": now_iso,
        }
        try:
            http_requests.post(
                f"{REDIS_SERVICE_URL}/redis/set",
                json={
                    "key": profile_key,
                    "value": json.dumps(profile_payload),
                    "db": 1,
                    "ttl": 1800,
                },
                timeout=3,
            )
        except Exception as exc:
            print(f"⚠️ student:profile cache write failed | identity={identity} | error={exc}")

    return {
        "success": True,
        "exists": total_bookings > 0,
        "total_bookings": total_bookings,
        "cache_hit": False,
    }


@app.get("/employees/list")
def list_employees():
    """Return list of employees - CACHED in Redis for 1 hour"""
    cache_key = "employee_list"
    
    cached_data = redis_get_safe(cache_key)
    if cached_data:
        print("🚀 Redis Cache Hit: employees-list")
        return {"employees": json.loads(cached_data)}
    
    print("❄️ Cache Miss: Updating Employee Redis Cache")
    employees = EMPLOYEE_EMAILS
    redis_setex_safe(cache_key, TTL_EMPLOYEES, json.dumps(employees))
    
    return {"employees": employees}


@app.get("/test/employees")
def test_employees():
    """Debug endpoint to verify EMPLOYEE_EMAILS is populated"""
    return {
        "count": len(EMPLOYEE_EMAILS),
        "employees": EMPLOYEE_EMAILS,
        "sample": EMPLOYEE_EMAILS[:3] if len(EMPLOYEE_EMAILS) > 0 else []
    }


@app.get("/employees/search")
def search_employees(q: str = "", user=Depends(get_current_user)):
    """Search employees by email prefix (for autocomplete). Admin only."""
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    
    if not q:
        return {"employees": EMPLOYEE_EMAILS}
    
    query = q.lower()
    filtered = [email for email in EMPLOYEE_EMAILS if query in email.lower()]
    return {"employees": filtered}


@app.get("/meeting/slots")
def meeting_slots(date: str):
    """
    Return available slots for a date.

    Flow:
      1. Check redis_service (DB 8) for cached slot availability (TTL 5 min).
      2. On miss, scan DynamoDB to compute availability.
      3. Store result back in redis_service.
      4. Any slot that is currently locked (meeting:lock:slot:*) is shown as
         unavailable in the response so the UI reflects real-time holds.
    """
    # 1. Check redis_service availability cache
    cached = meeting_slots_cache_get(date)
    if cached:
        print(f"🚀 Redis Cache Hit (redis_service DB 8): slots for {date}")
        cached_slots = cached.get("slots") if isinstance(cached, dict) else None
        if isinstance(cached_slots, list):
            for slot in cached_slots:
                if not isinstance(slot, dict) or not slot.get("available"):
                    continue
                start = slot.get("start")
                if not isinstance(start, str):
                    continue
                slot_key = f"{date}:{start}:{GRID_MINUTES}"
                if _rs_exists(_meeting_slot_lock_key(slot_key)):
                    slot["available"] = False
        return cached

    # 2. Cache miss — compute from DynamoDB
    print(f"❄️ Cache Miss: Scanning DynamoDB for slots on {date}")
    try:
        check_date = datetime.fromisoformat(date).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    bookings = scan_all_bookings(
        filter_expression=boto3.dynamodb.conditions.Attr("booking_date").eq(date)
    )
    occupied = []
    for b in bookings:
        if b.get("status") not in ("CANCELLED", "REJECTED"):
            start = b.get("start_ts", 0)
            dur = b.get("duration_minutes", 0)
            occupied.append((start, start + dur))

    slots = []
    current = to_minutes(BUSINESS_START.strftime("%H:%M"))
    end_mins = to_minutes(BUSINESS_END.strftime("%H:%M"))

    while current < end_mins:
        slot_end = current + GRID_MINUTES
        available = True
        for (occ_start, occ_end) in occupied:
            if not (slot_end <= occ_start or current >= occ_end):
                available = False
                break

        # Also mark as unavailable if the slot is currently locked in Redis
        slot_key = f"{date}:{from_minutes(current)}:{GRID_MINUTES}"
        if available and _rs_exists(_meeting_slot_lock_key(slot_key)):
            available = False  # held by another user in checkout

        slots.append({"start": from_minutes(current), "available": available})
        current += GRID_MINUTES

    result = {"date": date, "slots": slots}
    
    # 3. Store in redis_service (DB 8) for 5 minutes
    meeting_slots_cache_set(date, result)

    return result


@app.get("/pricing-model/status")
def pricing_model_status(user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    model = get_cached_pricing_model() or get_or_train_pricing_model()
    return {"success": True, "model": model}


@app.post("/pricing-model/train")
def pricing_model_train(user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    pricing_model = get_or_train_pricing_model(force_retrain=True)
    suggestion_model = get_or_train_auto_suggestion_model(force_retrain=True)
    return {
        "success": True,
        "message": "Pricing and Auto Suggestion models training completed",
        "pricing_model": pricing_model,
        "suggestion_model": {
            "version": suggestion_model.get("version"),
            "status": suggestion_model.get("status"),
            "training_samples": suggestion_model.get("training_samples"),
            "duration_r2": suggestion_model.get("duration_r2"),
            "complexity_lr_accuracy": suggestion_model.get("complexity_lr_accuracy"),
            "complexity_rf_accuracy": suggestion_model.get("complexity_rf_accuracy"),
        },
    }


@app.post("/meeting/agentic-suggestions")
def meeting_agentic_suggestions(req: AutoSuggestionRequest):
    """
    Auto Suggestion endpoint.

    Returns ML-driven suggestions for duration and complexity based on:
      - Student's booking history (DynamoDB)
      - Free-text booking reason semantics (BGE-M3 via RAG service)
      - Current day demand (DynamoDB)
      - Lead time

    Models:
      Ridge Regression    → suggested_duration_minutes
      LogisticRegression  → suggested_complexity  (primary)
      RandomForest        → complexity confidence signal (secondary)

    If the auto suggestion model has insufficient data it returns nulls
    gracefully so the frontend can fall back to defaults.
    """
    print(f"\n{'='*80}")
    print(f"🌐 /meeting/agentic-suggestions ENDPOINT CALLED")
    print(f"{'='*80}")
    print(f"📋 Request: email={req.email} date={req.date} booking_reason={req.booking_reason}")
    email = (req.email or "").strip().lower() or None
    booking_reason = (req.booking_reason or "").strip() or None
    print(f"✅ Parsed email={email} booking_reason_len={len(booking_reason) if booking_reason else 0}")

    # ── Lead time ─────────────────────────────────────────────────────────────
    lead_time_days = 0
    if req.date:
        try:
            lead_time_days = (
                datetime.fromisoformat(req.date).date() - datetime.utcnow().date()
            ).days
            lead_time_days = max(lead_time_days, 0)
            print(f"✅ Lead time calculated: {lead_time_days} days")
        except ValueError as e:
            print(f"⚠️ Lead time parse error: {e}")
            pass

    # ── Demand signal ─────────────────────────────────────────────────────────
    current_day_demand = 0
    if req.date:
        print(f"📊 Scanning DynamoDB for demand on {req.date}")
        day_bookings = scan_all_bookings(
            filter_expression=boto3.dynamodb.conditions.Attr("booking_date").eq(req.date)
        )
        current_day_demand = count_active_bookings(day_bookings)
        print(f"✅ Current day demand: {current_day_demand} active bookings")

    # ── Existing customer check ───────────────────────────────────────────────
    is_existing = False
    if email:
        print(f"🔎 Checking if {email} is existing customer")
        marker = meeting_student_marker_get(email)
        if marker:
            is_existing = True
            print(f"✅ Marker found | existing=True")
        else:
            print(f"⚠️ No marker, scanning DynamoDB for previous bookings...")
            existing_bookings = scan_all_bookings(
                filter_expression=boto3.dynamodb.conditions.Attr("created_by").eq(email)
            )
            is_existing = count_active_bookings(existing_bookings) > 0
            print(f"✅ DynamoDB check | existing={is_existing} | total_bookings={len(existing_bookings)}")
    else:
        print(f"⚠️ No email provided | is_existing=False")

    # ── Load / train suggestion model ────────────────────────────────────────
    print(f"🤖 Loading/training auto suggestion model...")
    model = get_or_train_auto_suggestion_model()
    print(f"✅ Model loaded | status={model.get('status')} | training_samples={model.get('training_samples')}")

    # ── RAG context (past session summaries) ────────────────────────────────
    rag_context = ""
    if is_existing and email:
        print(f"📚 Fetching RAG context for {email}...")
        try:
            rag_context = get_rag_context_for_suggestion(email, booking_reason or "")
            print(f"✅ RAG context retrieved | len={len(rag_context)} chars")
        except Exception as rag_err:
            print(f"⚠️ RAG fetch failed, continuing without context: {type(rag_err).__name__}: {rag_err}")
            rag_context = ""
    else:
        print(f"⚠️ RAG context skipped | is_existing={is_existing} has_email={email is not None}")

    # ── Load / train suggestion model ─────────────────────────────────────────
    print(f"🤖 Loading/training auto suggestion model...")
    model = get_or_train_auto_suggestion_model()
    print(f"✅ Model loaded | status={model.get('status')} | training_samples={model.get('training_samples')}")

    # ── RAG context (past session summaries) ──────────────────────────────────
    rag_context = ""
    if is_existing and email:
        print(f"📚 Fetching RAG context for {email}...")
        try:                                          # ← ADD try/except here
            rag_context = get_rag_context_for_suggestion(email, booking_reason or "")
            print(f"✅ RAG context retrieved | len={len(rag_context)} chars")
        except Exception as rag_err:
            print(f"⚠️ RAG fetch failed, continuing without context: {type(rag_err).__name__}: {rag_err}")
            rag_context = ""                          # ← graceful degradation
    else:
        print(f"⚠️ RAG context skipped | is_existing={is_existing} has_email={email is not None}")

    # ── Run inference ──────────────────────────────────────────────────────────
    print(f"\n--- INFERENCE PHASE ---")
    try:
        suggestions = infer_auto_suggestions(
        model_data=model,
        booking_reason=booking_reason,
        #rag_context=rag_context,
        current_day_demand=current_day_demand,
        lead_time_days=lead_time_days,
        is_existing=is_existing,
    )
        print(f"✅ Inference complete | suggestions={suggestions}")
    except Exception as inf_err:
        print(f"❌ Inference failed: {type(inf_err).__name__}: {inf_err}")
        import traceback
        traceback.print_exc()
        suggestions = {
            "model_status": "unavailable",
            "suggested_duration_minutes": None,
            "suggested_complexity": None,
            "suggested_slot": None,
        }    
        
    # ── KAFKA: publish meeting.suggestion.requested ───────────────────────────
    request_id = str(uuid.uuid4())
    print(f"\n📤 Publishing Kafka event | topic=meeting.suggestion.requested | request_id={request_id}")
    try:
        _kafka_publish("meeting.suggestion.requested", {
            "request_id":             request_id,
            "email":                  email,
            "date":                   req.date,
            "booking_reason":         booking_reason,
            "suggestions":            suggestions,
            "current_day_demand":     current_day_demand,
            "lead_time_days":         lead_time_days,
            "is_existing_customer":   is_existing,
            "timestamp":              datetime.utcnow().isoformat()
        })
        print(f"✅ Kafka event published")
    except Exception as kafka_err:
        print(f"⚠️ Kafka publish failed (non-blocking): {type(kafka_err).__name__}: {kafka_err}")

    print(f"{'='*80}")
    print(f"✅ /meeting/agentic-suggestions RESPONSE READY")
    print(f"{'='*80}\n")
    return {
        "success": True,
        "email": email,
        "date": req.date,
        "request_id": request_id,
        "suggestions": suggestions,
        "rag_context": rag_context,
        "context": {
            "current_day_demand": current_day_demand,
            "lead_time_days": lead_time_days,
            "is_existing_customer": is_existing,
            "booking_reason_provided": booking_reason is not None,
        },
    }


# ==========================================
# NEW: PENDING HOLD ENDPOINT
# ==========================================

@app.post("/meeting-hold")
def meeting_hold(req: PendingHoldRequest, user=Depends(get_current_user)):
    """
    Step 1 of booking flow — place a temporary pending hold on a slot before
    the user opens Razorpay checkout.

    Flow:
      1. Validate date/time.
      2. Try to acquire a short-TTL slot lock in Redis (prevents double-booking).
      3. Store a pending hold keyed by user_id (TTL 90 sec).
      4. Return hold details so the frontend can proceed to Razorpay.

    The slot lock expires automatically (TTL_SLOT_LOCK = 20 sec) if the user
    abandons checkout. The pending hold expires after TTL_PENDING_HOLD = 90 sec.
    Both are cleaned up explicitly by /meeting-book on success or /meeting-hold-release.
    """
    try:
        dt_start = datetime.fromisoformat(f"{req.date}T{req.start_time}")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date/time format")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    is_internal = (req.booking_type == "internal")
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)
    min_date = today if is_internal else tomorrow
    if dt_start < min_date:
        detail = (
            "Same-day bookings are available for internal users only"
            if dt_start >= today
            else "Bookings must be at least 1 day ahead"
        )
        raise HTTPException(status_code=400, detail=detail)

    slot_key = f"{req.date}:{req.start_time}:{req.duration_minutes}"

    # Try to acquire slot lock
    try:
        locked = meeting_slot_lock_acquire(slot_key, user["username"])
    except RuntimeError as exc:
        print(f"❌ Pending hold failed | slot={slot_key} error={exc}")
        raise HTTPException(status_code=503, detail="Booking lock service is temporarily unavailable. Please try again.")
    if not locked:
        holder = meeting_slot_lock_held_by(slot_key)
        if holder and holder != user["username"]:
            raise HTTPException(
                status_code=409,
                detail="This slot is currently being booked by another user. Please try a different slot or wait a moment."
            )
        # If we already hold the lock (re-entrant), proceed.

    # Store pending hold
    hold_data = {
        "user_id": user["username"],
        "date": req.date,
        "start_time": req.start_time,
        "duration_minutes": req.duration_minutes,
        "complexity": req.complexity,
        "booking_type": req.booking_type,
        "held_at": now.isoformat(),
        "slot_key": slot_key,
        "expires_in_seconds": TTL_PENDING_HOLD,
    }
    meeting_pending_hold_set(user["username"], hold_data)

    print(f"✅ Pending hold placed | user={user['username']} slot={slot_key}")

    return {
        "success": True,
        "message": "Slot temporarily held. Please complete payment within 90 seconds.",
        "hold": hold_data,
    }


@app.delete("/meeting-hold-release")
def meeting_hold_release(user=Depends(get_current_user)):
    """
    Explicitly release the pending hold and slot lock for the current user
    (called when the user cancels Razorpay checkout without paying).
    """
    hold = meeting_pending_hold_get(user["username"])
    if hold:
        meeting_slot_lock_release(hold.get("slot_key", ""))
    meeting_pending_hold_clear(user["username"])
    return {"success": True, "message": "Hold released."}


@app.get("/meeting-price-preview")
def meeting_price_preview(
    date: str,
    start_time: str,
    duration_minutes: int,
    complexity: str = "Medium",
    booking_type: str = "external",
    email: Optional[str] = None,
):

    """
    Returns the calculated price for a meeting WITHOUT creating a booking.
    Frontend should call this first to get the price, then create a
    Razorpay order, and finally submit /meeting-book with payment details.
    """
    try:
        dt_start = datetime.fromisoformat(f"{date}T{start_time}")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date/time format")

    now = datetime.utcnow()
    is_internal = (booking_type == "internal") or is_org_email(email)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)
    min_date = today if is_internal else tomorrow
    if dt_start < min_date:
        detail = (
            "Same-day bookings are available for internal users only"
            if dt_start >= today
            else "Bookings must be at least 1 day ahead"
        )
        raise HTTPException(status_code=400, detail=detail)

    if duration_minutes <= 0:
        raise HTTPException(status_code=400, detail="Duration must be greater than 0")

    lookup_user = (email or "").strip().lower() or "guest"
    lead_time_days = (dt_start.date() - now.date()).days
    start_minutes = to_minutes(start_time)

    day_bookings = scan_all_bookings(
        filter_expression=boto3.dynamodb.conditions.Attr("booking_date").eq(date)
    )
    current_day_demand = count_active_bookings(day_bookings)

    marker = meeting_student_marker_get(lookup_user)
    if marker:
        is_existing_customer = True
    else:
        existing_user_bookings = scan_all_bookings(
            filter_expression=boto3.dynamodb.conditions.Attr("created_by").eq(lookup_user)
        )
        is_existing_customer = count_active_bookings(existing_user_bookings) > 0

    heuristic_price, heuristic_breakdown = calculate_dynamic_price(
        is_existing=is_existing_customer,
        duration_minutes=duration_minutes,
        complexity=complexity,
        day_bookings_count=current_day_demand,
        lead_time_days=lead_time_days,
        start_minutes=start_minutes,
        is_internal=is_internal,
    )

    if is_internal:
        return {
            "price": INTERNAL_DOMAIN_PRICE,
            "price_paise": INTERNAL_DOMAIN_PRICE * 100,
            "heuristic_price": INTERNAL_DOMAIN_PRICE,
            "ml_predicted_price": None,
            "pricing_strategy": "internal_fixed_price",
            "heuristic_breakdown": heuristic_breakdown,
        }

    model = get_or_train_pricing_model()
    feature_vector = build_feature_vector(
        duration_minutes=duration_minutes,
        complexity=complexity,
        demand_score=current_day_demand,
        lead_time_days=lead_time_days,
        team_size=1,
        is_internal=is_internal,
        is_existing=is_existing_customer,
        start_minutes=start_minutes,
    )
    ml_price = predict_price_from_model(model, feature_vector)
    price, pricing_context = optimize_predicted_price(heuristic_price, ml_price, model)

    return {
        "price": price,
        "price_paise": price * 100,
        "heuristic_price": heuristic_price,
        "ml_predicted_price": pricing_context["ml_predicted_price"],
        "pricing_strategy": pricing_context["strategy"],
        "heuristic_breakdown": heuristic_breakdown,
    }


@app.post("/meeting-book")
@app.post("/meeting/book")
def meeting_book(req: BookingRequest):
    """
    Create a new booking with Dynamic Pricing and Heuristic optimization.

    Full Redis → DynamoDB flow:
      1. Verify Razorpay payment signature.
      2. Acquire slot lock in Redis via redis_service (DB 8) — rejects if another
         user holds the lock (double-booking guard).
      3. Write booking to DynamoDB (authoritative source of truth).
      4. On DynamoDB success:
           a. Invalidate slot availability cache for the date (redis_service DB 8).
           b. Invalidate per-user booking cache (redis_service DB 8).
           c. Release slot lock.
           d. Clear pending hold.
      5. Send email notifications.
    """
    is_internal = (req.booking_type == "internal") or is_org_email(req.email)
    booking_actor = (req.email or "").strip().lower() or "guest"
    organizer = None
    normalized_employee_emails: List[str] = []

    if req.duration_minutes <= 0:
        raise HTTPException(status_code=400, detail="Duration must be greater than 0")
    
    # 1. VALIDATION FOR INTERNAL BOOKINGS
    if is_internal:
        for emp_email in (req.employee_emails or []):
            normalized_email = (emp_email or "").strip().lower()
            if not is_valid_email(normalized_email):
                raise HTTPException(status_code=400, detail=f"Invalid employee email format: {emp_email}")
            normalized_employee_emails.append(normalized_email)

        normalized_employee_emails = list(dict.fromkeys(normalized_employee_emails))
        
        has_external_attendee = any(
            not email.endswith("@chakorahub.com") 
            for email in normalized_employee_emails
        )
        if has_external_attendee:
            is_internal = False

        organizer = req.email or booking_actor

    # 2. QUICK TOUCHPOINT HEURISTIC
    final_duration = int(req.duration_minutes)
    if req.issue_description:
        if is_quick_touchpoint(req.issue_description, req.complexity):
            final_duration = 5

    # 3. DATE/TIME VALIDATION
    try:
        dt_start = datetime.fromisoformat(f"{req.date}T{req.start_time}")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date/time format")

    now = datetime.utcnow()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)
    min_date = today if is_internal else tomorrow
    if dt_start < min_date:
        detail = (
            "Same-day bookings are available for internal users only"
            if dt_start >= today
            else "Bookings must be at least 1 day ahead"
        )
        raise HTTPException(status_code=400, detail=detail)

    # 4. PAYMENT VERIFICATION (local signature check)
    payment_id = (req.payment_id or "").strip()
    order_id   = (req.order_id   or "").strip()
    signature  = (req.signature  or "").strip()

    if is_internal:
        booking_payment_status = "NOT_REQUIRED"
    else:
        if not all([payment_id, order_id, signature]):
            print("⚠️ External booking missing payment fields")
            raise HTTPException(
                status_code=400,
                detail="Payment required. Please complete Razorpay payment before booking."
            )

        if not _verify_razorpay_signature(order_id, payment_id, signature):
            print(f"⚠️ Razorpay signature mismatch | order_id={order_id} payment_id={payment_id}")
            raise HTTPException(
                status_code=400,
                detail="Payment verification failed. Please try again."
            )
        booking_payment_status = "PAID"

    # 5. SLOT LOCK (double-booking guard via redis_service DB 8)
    slot_key = f"{req.date}:{req.start_time}:{final_duration}"
    pending_hold = meeting_pending_hold_get(booking_actor)
    has_matching_pending_hold = meeting_pending_hold_matches_slot(
        pending_hold,
        date=req.date,
        start_time=req.start_time,
        duration_minutes=final_duration,
    )

    print(
        f"ℹ️ Booking lock check | actor={booking_actor} slot={slot_key} "
        f"has_pending_hold={has_matching_pending_hold}"
    )

    lock_holder = meeting_slot_lock_held_by(slot_key)
    if lock_holder and lock_holder != booking_actor:
        holder_pending_hold = meeting_pending_hold_get(lock_holder)
        holder_has_matching_hold = meeting_pending_hold_matches_slot(
            holder_pending_hold,
            date=req.date,
            start_time=req.start_time,
            duration_minutes=final_duration,
        )

        if holder_has_matching_hold:
            print(f"⚠️ Slot lock busy | slot={slot_key} holder={lock_holder}")
            raise HTTPException(
                status_code=409,
                detail="This slot was just taken by another user. Please select a different slot."
            )

        print(f"⚠️ Releasing stale slot lock | slot={slot_key} holder={lock_holder}")
        meeting_slot_lock_release(slot_key)
        lock_holder = None

    if not lock_holder:
        try:
            lock_acquired = meeting_slot_lock_acquire(slot_key, booking_actor)
        except RuntimeError as exc:
            print(f"❌ Slot lock infrastructure error | actor={booking_actor} slot={slot_key} error={exc}")
            raise HTTPException(
                status_code=503,
                detail="Booking lock service is temporarily unavailable. Please try again."
            )
        if not lock_acquired:
            lock_holder = meeting_slot_lock_held_by(slot_key)
            print(
                f"⚠️ Slot lock acquire returned False | actor={booking_actor} "
                f"slot={slot_key} lock_holder={lock_holder}"
            )

            # Fallback for transient redis_service inconsistency: a stale exists/set
            # race can return False while no reliable holder is visible.
            if not lock_holder:
                print(f"⚠️ Retrying slot lock after stale/empty holder | slot={slot_key}")
                meeting_slot_lock_release(slot_key)
                try:
                    lock_acquired = meeting_slot_lock_acquire(slot_key, booking_actor)
                except RuntimeError as exc:
                    print(f"❌ Slot lock retry infrastructure error | actor={booking_actor} slot={slot_key} error={exc}")
                    raise HTTPException(
                        status_code=503,
                        detail="Booking lock service is temporarily unavailable. Please try again."
                    )
                if lock_acquired:
                    print(f"✅ Slot lock acquired on retry | actor={booking_actor} slot={slot_key}")
                else:
                    lock_holder = meeting_slot_lock_held_by(slot_key)
                    print(
                        f"❌ Retry slot lock failed | actor={booking_actor} "
                        f"slot={slot_key} lock_holder={lock_holder}"
                    )

            if lock_acquired:
                lock_holder = booking_actor

            if lock_holder and lock_holder != booking_actor:
                print(f"❌ 409 lock owned by another user | actor={booking_actor} holder={lock_holder} slot={slot_key}")
                raise HTTPException(
                    status_code=409,
                    detail="This slot was just taken by another user. Please select a different slot."
                )

            if not lock_acquired:
                print(f"❌ 409 lock could not be secured | actor={booking_actor} slot={slot_key}")
            raise HTTPException(
                status_code=409,
                detail="Could not secure the slot for booking. Please try again."
            )

        if has_matching_pending_hold:
            print(f"🔒 Reacquired expired slot lock from pending hold | user={booking_actor} slot={slot_key}")

    # 6. DEMAND / PRICING
    day_bookings = scan_all_bookings(
        filter_expression=boto3.dynamodb.conditions.Attr("booking_date").eq(req.date)
    )
    current_day_demand = count_active_bookings(day_bookings)

    marker = meeting_student_marker_get(booking_actor)
    if marker:
        is_existing_customer = True
    else:
        existing_user_bookings = scan_all_bookings(
            filter_expression=boto3.dynamodb.conditions.Attr("created_by").eq(booking_actor)
        )
        is_existing_customer = count_active_bookings(existing_user_bookings) > 0

    lead_time_days = (dt_start.date() - now.date()).days
    team_size = max(len(normalized_employee_emails), 1) if is_internal else 1
    start_minutes = to_minutes(req.start_time)

    heuristic_price, heuristic_breakdown = calculate_dynamic_price(
        is_existing=is_existing_customer,
        duration_minutes=final_duration, 
        complexity=req.complexity, 
        day_bookings_count=current_day_demand, 
        lead_time_days=lead_time_days,
        start_minutes=start_minutes,
        is_internal=is_internal,
    )

    if is_internal:
        model = {"version": PRICING_MODEL_VERSION}
        price = INTERNAL_DOMAIN_PRICE
        pricing_context = {
            "strategy": "internal_fixed_price",
            "confidence": 1.0,
            "ml_predicted_price": None,
            "training_samples": 0,
            "model_r2": 0.0,
        }
    else:
        model = get_or_train_pricing_model()
        feature_vector = build_feature_vector(
            duration_minutes=final_duration,
            complexity=req.complexity,
            demand_score=current_day_demand,
            lead_time_days=lead_time_days,
            team_size=team_size,
            is_internal=is_internal,
            is_existing=is_existing_customer,
            start_minutes=start_minutes,
        )
        ml_price = predict_price_from_model(model, feature_vector)
        price, pricing_context = optimize_predicted_price(heuristic_price, ml_price, model)

    # 7. BUILD & WRITE DYNAMO ITEM (source of truth)
    booking_id = str(uuid.uuid4())
    item = {
        "bookingId": booking_id,
        "student_email": req.email or "",
        "booking_date": req.date,
        "start_ts": start_minutes,
        "start_time": req.start_time,
        "duration_minutes": final_duration,
        "price": decimal.Decimal(str(price)),
        "heuristic_price": decimal.Decimal(str(heuristic_price)),
        "complexity": req.complexity,
        "is_existing": is_existing_customer,
        "status": "PENDING",
        "payment_status": booking_payment_status,
        "razorpay_payment_id": payment_id,
        "razorpay_order_id": order_id,
        "created_at": datetime.utcnow().isoformat(),
        "created_by": booking_actor,
        "booking_type": req.booking_type,
        "pricing_strategy": pricing_context["strategy"],
        "pricing_confidence": decimal.Decimal(str(pricing_context["confidence"])),
        "pricing_model_version": model.get("version", PRICING_MODEL_VERSION),
        "pricing_model_r2": decimal.Decimal(str(pricing_context["model_r2"])),
        "pricing_training_samples": pricing_context["training_samples"],
        "ml_demand_score": current_day_demand,
        "ml_lead_time": lead_time_days,
        "heuristic_demand_factor": decimal.Decimal(str(heuristic_breakdown["demand_factor"])),
        "heuristic_lead_time_factor": decimal.Decimal(str(heuristic_breakdown["lead_time_factor"])),
        "heuristic_slot_time_factor": decimal.Decimal(str(heuristic_breakdown["slot_time_factor"])),
        "heuristic_supply_factor": decimal.Decimal(str(heuristic_breakdown["supply_factor"])),
    }

    if pricing_context["ml_predicted_price"] is not None:
        item["ml_predicted_price"] = decimal.Decimal(str(pricing_context["ml_predicted_price"]))
    if pricing_context.get("bounded_ml_price") is not None:
        item["ml_bounded_price"] = decimal.Decimal(str(pricing_context["bounded_ml_price"]))

    try:
        bookings_table.put_item(Item=item)  
        print(f"✅ DynamoDB booking written | booking_id={booking_id}")
    except Exception as dynamo_err:
        # DynamoDB write failed — release lock so other users are not blocked
        meeting_slot_lock_release(slot_key)
        print(f"❌ DynamoDB write failed: {dynamo_err}")
        raise HTTPException(status_code=500, detail="Failed to save booking. Please try again.")

    meeting_student_marker_set(
        booking_actor,
        {
            "first_seen": marker.get("first_seen") if isinstance(marker, dict) else datetime.utcnow().isoformat(),
            "last_booking_id": booking_id,
            "last_booking_date": req.date,
            "last_booking_time": req.start_time,
            "last_seen": datetime.utcnow().isoformat(),
        },
    )

    # 8. REDIS CACHE CLEANUP (post-DynamoDB success)
    meeting_slots_cache_invalidate(req.date)
    meeting_user_cache_invalidate(booking_actor)
    meeting_slot_lock_release(slot_key)
    meeting_pending_hold_clear(booking_actor)

    teams_link = None

    # Publish booking event; ms365_service creates Teams link asynchronously.
    _kafka_publish("meeting.booked", {
        "booking_id":       booking_id,
        "student_email":    req.email or booking_actor,
        "student_name":     booking_actor,
        "date":             req.date,
        "start_time":       req.start_time,
        "duration_minutes": final_duration,
        "complexity":       req.complexity,
        "booking_type":     req.booking_type,
        "price":            float(price),
        "payment_id":       payment_id,
        "order_id":         order_id,
        "purpose":          req.purpose or "",
        "correlation_id":   booking_id,
        "published_at":     datetime.utcnow().isoformat(),
    })

    # Email is sent asynchronously by billing_service after teams.link.created.

    return {
        "success": True,
        "booking_id": booking_id,
        "price": price,
        "heuristic_price": heuristic_price,
        "requested_duration_minutes": req.duration_minutes,
        "final_duration_minutes": final_duration,
        "quick_touchpoint_applied": final_duration != req.duration_minutes,
        "ml_predicted_price": pricing_context["ml_predicted_price"],
        "pricing_strategy": pricing_context["strategy"],
        "pricing_confidence": pricing_context["confidence"],
        "training_samples": pricing_context["training_samples"],
        "model_r2": pricing_context["model_r2"],
        "heuristic_breakdown": heuristic_breakdown,
        "message": f"✅ Booking confirmed! Booking ID: {booking_id}",
        "teams_link": teams_link or None
    }


@app.post("/meeting/update-purpose")
def meeting_update_purpose(req: BookingPurposeRequest):
    booking_id = (req.booking_id or "").strip()
    purpose = (req.purpose or "").strip()

    if not booking_id:
        raise HTTPException(status_code=400, detail="booking_id is required")
    if not purpose:
        raise HTTPException(status_code=400, detail="purpose is required")

    try:
        existing = bookings_table.get_item(Key={"bookingId": booking_id}).get("Item")
        if not existing:
            raise HTTPException(status_code=404, detail="Booking not found")

        bookings_table.update_item(
            Key={"bookingId": booking_id},
            UpdateExpression="SET purpose = :purpose",
            ExpressionAttributeValues={":purpose": purpose},
        )

        student_email = (existing.get("student_email") or "").strip()
        teams_link = existing.get("teams_link") or ""
        email_sent = False

        if student_email and teams_link:
            _kafka_publish("teams.link.created", {
                "booking_id": booking_id,
                "correlation_id": booking_id,
                "student_email": student_email,
                "student_name": existing.get("created_by", "student"),
                "date": existing.get("booking_date", ""),
                "start_time": existing.get("start_time", ""),
                "duration_minutes": int(existing.get("duration_minutes", 0) or 0),
                "price": float(to_float(existing.get("price"), 0.0)),
                "complexity": existing.get("complexity", "Medium"),
                "booking_type": existing.get("booking_type", "external"),
                "meeting_link": teams_link,
                "meeting_id": existing.get("meeting_id", ""),
                "purpose": purpose,
                "payment_id": existing.get("razorpay_payment_id", ""),
                "order_id": existing.get("razorpay_order_id", ""),
                "source": "meeting_service",
                "published_at": datetime.utcnow().isoformat(),
            })
            email_sent = True

        print(f"✅ Booking purpose updated | booking_id={booking_id}")
        return {
            "success": True,
            "booking_id": booking_id,
            "purpose": purpose,
            "email_sent": email_sent,
            "teams_link": teams_link or None,
            "email_error": None,
        }
    except HTTPException:
        raise
    except Exception as exc:
        print(f"❌ Booking purpose update failed | booking_id={booking_id} error={exc}")
        raise HTTPException(status_code=500, detail="Failed to update booking purpose")


# STUDENT: GET /meeting-mybookings
# @app.get("/meeting-mybookings")
# def meeting_my_bookings(user=Depends(get_current_user)):
#     if user["role"] != "student":
#         raise HTTPException(status_code=403, detail="Students only")

#     # Try redis_service user cache first (DB 8)
#     cached = meeting_user_cache_get(user["username"])
#     if cached is not None:
#         print(f"🚀 Redis Cache Hit (redis_service DB 8): user bookings for {user['username']}")
#         return {"bookings": cached}

#     bookings = scan_all_bookings(
#         filter_expression=boto3.dynamodb.conditions.Attr("created_by").eq(user["username"])
#     )
#     bookings.sort(key=lambda x: (x.get("booking_date", ""), x.get("start_ts", 0)), reverse=True)
#     serialisable = jsonable_encoder(bookings, custom_encoder={decimal.Decimal: float})

#     # Cache for 2 min
#     meeting_user_cache_set(user["username"], serialisable)

#     return {"bookings": serialisable}

# DynamoDB setup
dynamodb = boto3.resource('dynamodb', region_name='eu-north-1')
bookings_table = dynamodb.Table('Bookings')

@app.get("/meeting/user-bookings")
async def get_user_bookings(
    email: Optional[str] = Query(None),
    phone: Optional[str] = Query(None)
):
    """
    Fetch all bookings for a user (by email or phone) from DynamoDB.
    Used by read-through cache strategy.
    """
    
    if not email and not phone:
        raise HTTPException(
            status_code=400,
            detail="Either 'email' or 'phone' parameter is required"
        )
    
    normalized_email = (email or "").strip().lower()
    search_value = normalized_email or phone
    search_field = 'student_email' if email else 'phone'

    # Read-through cache for the key requested in your current phase: meeting:user:{email}
    if normalized_email:
        cached_bookings = meeting_user_cache_get(normalized_email)
        if cached_bookings is not None:
            return {
                "success": True,
                "bookings": cached_bookings,
                "total": len(cached_bookings),
                "search_field": "student_email",
                "search_value": normalized_email,
                "source": "redis",
            }
    
    try:
        # Query DynamoDB
        response = bookings_table.scan(
            FilterExpression=Key(search_field).eq(search_value)
        )
        
        bookings = response.get('Items', [])
        
        # Handle pagination if needed
        while 'LastEvaluatedKey' in response:
            response = bookings_table.scan(
                FilterExpression=Key(search_field).eq(search_value),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            bookings.extend(response.get('Items', []))
        
        response_payload = {
            "success": True,
            "bookings": bookings,
            "total": len(bookings),
            "search_field": search_field,
            "search_value": search_value,
            "source": "dynamodb",
        }

        if normalized_email:
            serialisable_bookings = jsonable_encoder(bookings, custom_encoder={decimal.Decimal: float})
            meeting_user_cache_set(normalized_email, serialisable_bookings)
            response_payload["bookings"] = serialisable_bookings

        return response_payload
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )


# STUDENT / ADMIN: DELETE /meeting-cancel?booking_id=...
@app.delete("/meeting-cancel")
def meeting_cancel(booking_id: str, user=Depends(get_current_user)):
    resp = bookings_table.get_item(Key={"bookingId": booking_id})
    booking = resp.get("Item")

    if not booking:
        raise HTTPException(status_code=404, detail="Not found")

    if user["role"] == "student" and booking.get("created_by") != user["username"]:
        raise HTTPException(status_code=403, detail="Not your booking")

    bookings_table.update_item(
        Key={"bookingId": booking_id},
        UpdateExpression="SET #st = :s",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={":s": "CANCELLED"},
    )

    # Invalidate caches after cancellation
    booking_date = booking.get("booking_date", "")
    if booking_date:
        meeting_slots_cache_invalidate(booking_date)
    meeting_user_cache_invalidate(booking.get("created_by", user["username"]))

    return {"message": "Cancelled"}


# ==========================================
# NEW: REDIS DEBUG ENDPOINTS (dev only)
# ==========================================

@app.get("/meeting/redis/debug")
def meeting_redis_debug(user=Depends(get_current_user)):
    """
    Admin-only: show active meeting keys in redis_service DB 8.
    Useful for monitoring locks, holds, and cache state.
    """
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    try:
        resp = http_requests.get(
            f"{REDIS_SERVICE_URL}/redis/scan",
            params={"pattern": "meeting:*", "db": _MEETING_DB},
            timeout=3,
        )
        data = resp.json()
        return {
            "success": True,
            "meeting_db": _MEETING_DB,
            "active_keys": data.get("keys", []),
            "count": data.get("count", 0),
        }
    except Exception as exc:
        return {"success": False, "message": str(exc)}


@app.get("/meeting/redis/hold")
def meeting_redis_hold_status(user=Depends(get_current_user)):
    """Check current pending hold for the authenticated user."""
    hold = meeting_pending_hold_get(user["username"])
    return {
        "user": user["username"],
        "has_hold": hold is not None,
        "hold": hold,
    }

@app.post("/submit-booking-reason")
def submit_reason(payload: BookingReasonRequest):

    save_reason_to_s3(payload)

    return {
        "message": "Reason stored successfully"
    }

@app.post("/teams-transcript")
def teams_transcript(payload: TeamsTranscriptRequest):

    # ============================================
    # STORE RAW TRANSCRIPT IN S3
    # ============================================

    save_transcript_to_s3(payload)

    # ============================================
    # RAG INGESTION
    # ============================================

    result = ingest_transcript(
        student_email=payload.student_email,
        transcript_text=payload.transcript,
        booking_reason=payload.booking_reason,
        instructor_rating=payload.instructor_rating
    )

    return result

@app.get("/student-progress")
def student_progress(student_email: str):

    analysis = analyze_student_progress(
        student_email
    )

    return {
        "student_email": student_email,
        "progress_analysis": analysis
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)