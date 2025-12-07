# meeting_service.py
import os
import uuid
import math
import base64
import decimal
from datetime import datetime, time, timedelta
from typing import Optional, List
from flask import Flask
import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi.requests import Request
from fastapi import FastAPI, HTTPException, Header, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, EmailStr

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

# ------------------------------
# CONSTANTS
# ------------------------------
CHARSET = "UTF-8"

BASE_RATE = 575
COMPLEXITY_MULTIPLIER = {"Easy": 0.875, "Medium": 1.0, "Difficult": 1.125}
MINIMUM_CHARGE_EXISTING = 50

VALID_CREDENTIALS = {
    "student": "student",
    "admin": "admin",
}

BUSINESS_START = time(10, 0)
BUSINESS_END = time(19, 0)
GRID_MINUTES = 15

# ------------------------------
# FASTAPI APP & CORS
# ------------------------------
app = FastAPI(title="ChakoraHub Meeting Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------
# Mount Flask (NOT at root!)
# ------------------------------
flask_app = Flask(__name__)
app.mount("/web", flask_app)

# ------------------------------
# MODELS
# ------------------------------
class BookingRequest(BaseModel):
    email: Optional[EmailStr] = None
    date: str
    start_time: str          # "HH:MM"
    duration_minutes: int
    complexity: str = "Medium"


class AdminApproveRequest(BaseModel):
    booking_id: str
    action: str = "approve"   # "approve" or "reject"


class CancelResponse(BaseModel):
    message: str


# ------------------------------
# UTILITIES
# ------------------------------
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


def to_minutes(t: str) -> int:
    h, m = map(int, t.split(":"))
    return h * 60 + m


def minutes_to_hhmm(mins: int) -> str:
    return f"{mins // 60:02d}:{mins % 60:02d}"


def calculate_price(is_existing: bool, mins: int, complexity: str) -> int:
    if is_existing:
        return MINIMUM_CHARGE_EXISTING
    rate = BASE_RATE * COMPLEXITY_MULTIPLIER.get(complexity, 1.0)
    return max(math.ceil((mins / 60) * rate), MINIMUM_CHARGE_EXISTING)


def generate_slots_for_date(date_str: str):
    # 10:00 (600) to 19:00 (1140)
    slots = []
    start_min = 600
    end_min = 1140
    for m in range(start_min, end_min, GRID_MINUTES):
        slots.append({"start": minutes_to_hhmm(m), "available": True})
    return slots


def get_bookings_for_date(date_str: str):
    try:
        resp = bookings_table.query(
            IndexName="by_date",
            KeyConditionExpression=boto3.dynamodb.conditions.Key("booking_date").eq(date_str),
        )
        return resp.get("Items", [])
    except Exception as e:
        print("Dynamo query error:", e)
        return []


def slots_with_availability(date_str: str):
    slots = generate_slots_for_date(date_str)
    bookings = get_bookings_for_date(date_str)

    for b in bookings:
        if b.get("status") not in ["APPROVED", "CONFIRMED"]:
            continue

        b_start = to_minutes(b["start_time"])
        b_end = b_start + int(b["duration_minutes"])

        for s in slots:
            s_min = to_minutes(s["start"])
            if s_min < b_end and (s_min + GRID_MINUTES) > b_start:
                s["available"] = False

    return slots


# ------------------------------
# AUTH
# ------------------------------
def get_current_user(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Basic "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Authentication required",
                            headers={"WWW-Authenticate": "Basic"})

    try:
        decoded = base64.b64decode(authorization.split(" ")[1]).decode()
        username, password = decoded.split(":", 1)
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid auth header",
                            headers={"WWW-Authenticate": "Basic"})

    if username in VALID_CREDENTIALS and VALID_CREDENTIALS[username] == password:
        role = "admin" if username == "admin" else "student"
        return {"username": username, "role": role}

    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Invalid credentials",
                        headers={"WWW-Authenticate": "Basic"})


# ------------------------------
# EMAIL SENDER
# (same behaviour as your Lambda, just slightly tidied)
# ------------------------------
def send_booking_email(student_email: str,
                       details: dict,
                       is_admin: bool = False,
                       cc_admin: bool = False,
                       action_type: Optional[str] = None) -> bool:
    try:
        booking_id = details.get("booking_id", "N/A")
        student_name = details.get("student_name", "Student")
        date = details.get("date", "N/A")
        start_time = details.get("start_time", "N/A")
        duration = details.get("duration_minutes", "N/A")
        price = details.get("price", "N/A")
        complexity = details.get("complexity", "N/A")
        status_val = details.get("status", "PENDING")
        action_by = details.get("action_by", "Admin")

        if is_admin:
            # ADMIN EMAILS (new booking or action_taken)
            if action_type == "action_taken":
                subject = f"✅ Action Taken: Booking {status_val} - {booking_id[:8]}"
                if status_val == "APPROVED":
                    action_text = "approved"
                    color = "#28a745"
                    icon = "✅"
                else:
                    action_text = "rejected"
                    color = "#dc3545"
                    icon = "❌"

                msg = f"""
                <html>
                <body style="font-family: Arial, sans-serif; line-height: 1.6;">
                  <div style="max-width: 600px; margin: 0 auto; padding: 20px;
                              border: 1px solid #e0e0e0; border-radius: 10px;">
                    <h2 style="color: {color};">{icon} Booking {status_val}</h2>
                    <p>You have <strong>{action_text}</strong> the following booking:</p>
                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0;">
                      <h3 style="margin-top: 0;">📋 Booking Details</h3>
                      <p><strong>Booking ID:</strong> {booking_id}</p>
                      <p><strong>Student:</strong> {student_name}</p>
                      <p><strong>Student Email:</strong> {student_email}</p>
                      <p><strong>Date:</strong> {date}</p>
                      <p><strong>Time:</strong> {start_time}</p>
                      <p><strong>Duration:</strong> {duration} minutes</p>
                      <p><strong>Complexity:</strong> {complexity}</p>
                      <p><strong>Price:</strong> ₹{price}</p>
                      <p><strong>Status:</strong>
                         <span style="background: {color}; color: white; padding: 3px 8px; border-radius: 4px;">
                           {status_val}
                         </span>
                      </p>
                      <p><strong>Action by:</strong> {action_by}</p>
                      <p><strong>Action time:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>
                    </div>
                    <p style="font-size: 0.9em; color: #666;">
                      ✅ Student has been notified of this action.
                    </p>
                  </div>
                </body>
                </html>
                """
            else:
                subject = f"📅 New Booking Request - {booking_id[:8]}"
                msg = f"""
                <html>
                <body style="font-family: Arial, sans-serif; line-height: 1.6;">
                  <div style="max-width: 600px; margin: 0 auto; padding: 20px;
                              border: 1px solid #e0e0e0; border-radius: 10px;">
                    <h2 style="color: #667eea;">New Booking Request</h2>
                    <p style="color: #666;">
                        A student has submitted a new booking request that requires your review.
                    </p>
                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0;">
                      <h3 style="margin-top: 0;">📋 Booking Details</h3>
                      <p><strong>Booking ID:</strong> {booking_id}</p>
                      <p><strong>Student:</strong> {student_name}</p>
                      <p><strong>Student Email:</strong> {student_email}</p>
                      <p><strong>Date:</strong> {date}</p>
                      <p><strong>Time:</strong> {start_time}</p>
                      <p><strong>Duration:</strong> {duration} minutes</p>
                      <p><strong>Complexity:</strong> {complexity}</p>
                      <p><strong>Price:</strong> ₹{price}</p>
                      <p><strong>Status:</strong>
                        <span style="background: #fff3cd; padding: 3px 8px; border-radius: 4px;">
                          {status_val}
                        </span>
                      </p>
                    </div>
                    <p>Please review and take action on this booking request.</p>
                    <a href="{ADMIN_PANEL_URL}"
                       style="display: inline-block; background: #667eea; color: white;
                              padding: 10px 20px; text-decoration: none; border-radius: 5px; margin-top: 10px;">
                        Go to Admin Panel
                    </a>
                  </div>
                </body>
                </html>
                """

            ses.send_email(
                Source=ADMIN_EMAIL,
                Destination={"ToAddresses": [ADMIN_EMAIL]},
                Message={
                    "Subject": {"Data": subject, "Charset": CHARSET},
                    "Body": {"Html": {"Data": msg, "Charset": CHARSET}},
                },
            )

        else:
            # STUDENT EMAILS
            if status_val == "PENDING":
                h = "📝 Booking Submitted Successfully"
                m = "Your booking has been submitted and is awaiting admin approval."
                color = "#fff3cd"
                icon = "⏳"
            elif status_val == "APPROVED":
                h = "🎉 Booking Approved!"
                m = "Great news! Your booking has been approved."
                color = "#d4edda"
                icon = "✅"
            elif status_val == "REJECTED":
                h = "⚠️ Booking Rejected"
                m = "Your booking request has been rejected."
                color = "#f8d7da"
                icon = "❌"
            else:
                h = "Booking Update"
                m = f"Your booking status has changed to {status_val}."
                color = "#e2e3e5"
                icon = "📧"

            subject = f"{h.split(' ')[0]} - Booking #{booking_id[:8]}"

            note_html = ""
            if cc_admin and status_val in ["APPROVED", "REJECTED"]:
                note_html = f"""
                <div style='background: #e7f3ff; padding: 10px; border-radius: 5px;
                            border-left: 4px solid #007bff; margin: 15px 0;'>
                  <strong>Note:</strong> A copy of this notification has been sent to {ADMIN_EMAIL}.
                </div>
                """

            msg = f"""
            <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6;">
              <div style="max-width: 600px; margin: 0 auto; padding: 20px;
                          border: 1px solid #e0e0e0; border-radius: 10px;">
                <h2 style="color: #667eea;">{icon} {h}</h2>
                <p style="color: #666;">{m}</p>
                <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0;">
                  <h3 style="margin-top: 0;">📋 Booking Details</h3>
                  <p><strong>Booking ID:</strong> {booking_id}</p>
                  <p><strong>Date:</strong> {date}</p>
                  <p><strong>Time:</strong> {start_time}</p>
                  <p><strong>Duration:</strong> {duration} minutes</p>
                  <p><strong>Complexity:</strong> {complexity}</p>
                  <p><strong>Price:</strong> ₹{price}</p>
                  <p><strong>Status:</strong>
                     <span style="background: {color}; padding: 3px 8px; border-radius: 4px;">
                       {status_val}
                     </span>
                  </p>
                </div>
                {note_html}
                <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #e0e0e0;
                            font-size: 0.9em; color: #666;">
                  <p>Need help? Contact support at {ADMIN_EMAIL}</p>
                </div>
              </div>
            </body>
            </html>
            """

            ses.send_email(
                Source=ADMIN_EMAIL,
                Destination={"ToAddresses": [student_email]},
                Message={
                    "Subject": {"Data": subject, "Charset": CHARSET},
                    "Body": {"Html": {"Data": msg, "Charset": CHARSET}},
                },
            )

            # If CC requested, send separate admin notification
            if cc_admin and status_val in ["APPROVED", "REJECTED"]:
                admin_action_details = {
                    "booking_id": booking_id,
                    "student_name": student_name,
                    "date": date,
                    "start_time": start_time,
                    "duration_minutes": duration,
                    "price": price,
                    "complexity": complexity,
                    "status": status_val,
                    "action_by": action_by,
                }
                send_booking_email(student_email, admin_action_details,
                                   is_admin=True, action_type="action_taken")

        return True
    except Exception as e:
        print("Email error:", e)
        return False


# ------------------------------
# ENDPOINTS
# ------------------------------
@app.get("/health")
def health_check():
    return {"status": "ok", "service": "meeting", "time": datetime.utcnow().isoformat()}


# PUBLIC: GET /meeting-slots?date=YYYY-MM-DD
@app.get("/meeting-slots")
def get_slots(date: str):
    if not date:
        raise HTTPException(status_code=400, detail="date required")

    slots = slots_with_availability(date)
    return {"slots": slots}


# STUDENT: POST /meeting-book
@app.post("/meeting-book")
def book_meeting(req: BookingRequest, user=Depends(get_current_user)):
    if user["role"] != "student":
        raise HTTPException(status_code=403, detail="Students only")

    # (Optional) enforce "at least 1 day ahead" rule – you had this in older Lambda /book
    try:
        dt_start = datetime.fromisoformat(f"{req.date}T{req.start_time}")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date/time format")

    now = datetime.utcnow()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if dt_start < tomorrow:
        raise HTTPException(status_code=400, detail="Bookings must be 1 day ahead")

    dt_end = dt_start + timedelta(minutes=req.duration_minutes)
    if not (BUSINESS_START <= dt_start.time() <= BUSINESS_END and
            BUSINESS_START <= dt_end.time() <= BUSINESS_END and
            dt_start < dt_end):
        raise HTTPException(status_code=400, detail="Outside business hours")

    booking_id = str(uuid.uuid4())
    duration = int(req.duration_minutes)
    price = calculate_price(False, duration, req.complexity)

    item = {
        "bookingId": booking_id,
        "studentId": user["username"],
        "student_email": req.email or "",
        "booking_date": req.date,
        "start_ts": to_minutes(req.start_time),
        "start_time": req.start_time,
        "duration_minutes": duration,
        "price": decimal.Decimal(str(price)),
        "complexity": req.complexity,
        "is_existing": False,
        "status": "PENDING",
        "payment_status": "PENDING",
        "created_at": datetime.utcnow().isoformat(),
        "created_by": user["username"],
    }

    bookings_table.put_item(Item=item)

    details = {
        "booking_id": booking_id,
        "student_name": user["username"],
        "date": req.date,
        "start_time": req.start_time,
        "duration_minutes": duration,
        "price": price,
        "complexity": req.complexity,
        "status": "PENDING",
    }

    if req.email:
        send_booking_email(req.email, details, is_admin=False)
    send_booking_email(req.email or "", details, is_admin=True)

    return {
        "booking_id": booking_id,
        "price": price,
        "status": "PENDING",
        "message": "Booking submitted. Awaiting admin approval.",
    }


# STUDENT: GET /meeting-mybookings
@app.get("/meeting-mybookings")
def meeting_my_bookings(user=Depends(get_current_user)):
    if user["role"] != "student":
        raise HTTPException(status_code=403, detail="Students only")

    resp = bookings_table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr("created_by").eq(user["username"])
    )
    bookings = resp.get("Items", [])
    bookings.sort(key=lambda x: (x.get("booking_date", ""), x.get("start_ts", 0)), reverse=True)

    return {"bookings": jsonable_encoder(bookings, custom_encoder={decimal.Decimal: float})}


# STUDENT / ADMIN: DELETE /meeting-cancel?booking_id=...
@app.delete("/meeting-cancel")
def meeting_cancel(booking_id: str, user=Depends(get_current_user)):
    resp = bookings_table.get_item(Key={"bookingId": booking_id})
    booking = resp.get("Item")

    if not booking:
        raise HTTPException(status_code=404, detail="Not found")

    # Students can cancel only their own bookings
    if user["role"] == "student" and booking.get("created_by") != user["username"]:
        raise HTTPException(status_code=403, detail="Not your booking")

    bookings_table.update_item(
        Key={"bookingId": booking_id},
        UpdateExpression="SET #st = :s",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={":s": "CANCELLED"},
    )

    return {"message": "Cancelled"}


# ADMIN: GET /admin-bookings[?status=PENDING]
@app.get("/admin-bookings")
def admin_get_bookings(status: Optional[str] = None, user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    if status:
        response = bookings_table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr("status").eq(status)
        )
    else:
        response = bookings_table.scan()

    bookings = response.get("Items", [])
    bookings.sort(key=lambda x: (x.get("booking_date", ""), x.get("start_ts", 0)))

    return {"bookings": jsonable_encoder(bookings, custom_encoder={decimal.Decimal: float})}


# ADMIN: PUT /admin-approve
@app.put("/admin-approve")
def admin_approve(req: AdminApproveRequest, user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    booking_id = req.booking_id
    action = req.action or "approve"

    resp = bookings_table.get_item(Key={"bookingId": booking_id})
    booking = resp.get("Item")

    if not booking:
        raise HTTPException(status_code=404, detail="Not found")

    new_status = "APPROVED" if action == "approve" else "REJECTED"
    payment_status = "CONFIRMED" if action == "approve" else "REJECTED"

    bookings_table.update_item(
        Key={"bookingId": booking_id},
        UpdateExpression="SET #st = :s, payment_status = :p, approved_at = :a, approved_by = :u",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":s": new_status,
            ":p": payment_status,
            ":a": datetime.utcnow().isoformat(),
            ":u": user["username"],
        },
    )

    student_email = booking.get("student_email", "")

    if student_email:
        details = {
            "booking_id": booking_id,
            "student_name": booking.get("studentId", "Student"),
            "date": booking.get("booking_date", "N/A"),
            "start_time": booking.get("start_time", "N/A"),
            "duration_minutes": booking.get("duration_minutes", "N/A"),
            "price": float(booking.get("price", 0)),
            "complexity": booking.get("complexity", "Medium"),
            "status": new_status,
            "action_by": user["username"],
        }

        # STUDENT mail + CC note
        send_booking_email(student_email, details, is_admin=False, cc_admin=True)
        # ADMIN summary mail
        send_booking_email(student_email, details, is_admin=True, action_type="action_taken")
        msg = f"Booking {action}d successfully. Emails sent to student and admin."
    else:
        # No student email; still notify admin
        admin_details = {
            "booking_id": booking_id,
            "student_name": booking.get("studentId", "Student"),
            "date": booking.get("booking_date", "N/A"),
            "start_time": booking.get("start_time", "N/A"),
            "duration_minutes": booking.get("duration_minutes", "N/A"),
            "price": float(booking.get("price", 0)),
            "complexity": booking.get("complexity", "Medium"),
            "status": new_status,
            "action_by": user["username"],
            "note": "No student email available",
        }
        send_booking_email(ADMIN_EMAIL, admin_details, is_admin=True, action_type="action_taken")
        msg = f"Booking {action}d successfully. Admin notified (no student email)."

    return {"booking_id": booking_id, "status": new_status, "message": msg}

@app.middleware("http")
async def add_cors_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "*"
    return response
