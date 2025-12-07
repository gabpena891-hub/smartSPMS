import os
import logging
from datetime import date, datetime

from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS
from sqlalchemy import (Column, Date, DateTime, DECIMAL, ForeignKey, Integer,
                        String, Text, create_engine, func, text, case, and_, or_)
from sqlalchemy.orm import declarative_base, relationship, scoped_session, sessionmaker
from sqlalchemy.exc import IntegrityError

# Flask setup with CORS for local frontend (e.g., http://127.0.0.1:5500) and file://
app = Flask(__name__, static_folder=".", static_url_path="")
CORS(
    app,
    resources={r"/api/*": {"origins": ["*", "http://127.0.0.1:5500", "http://localhost:5500", "null"]}},
)

# Database configuration
db_url = os.environ.get("DATABASE_URL")
if db_url and db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)
if not db_url:
    db_url = "sqlite:///local.db"

engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=1800)
SessionLocal = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()

def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        return True, "tables ensured"
    except Exception as exc:
        logging.warning("DB init failed: %s", exc)
        return False, str(exc)


# Ensure tables exist (idempotent; safe for first run on Render/sqlite)
init_db()


# Static file serving for Render/static hosting
@app.route("/")
def serve_index():
    return app.send_static_file("index.html")


@app.route("/<path:path>")
def serve_static_file(path):
    if path.startswith("api/"):
        return abort(404)
    return send_from_directory(app.static_folder, path)

@app.route("/api/admin/init", methods=["POST", "GET"])
def admin_init():
    token = os.environ.get("ADMIN_INIT_TOKEN")
    if token:
        provided = request.headers.get("X-Admin-Init-Token") or request.args.get("token")
        if provided != token:
            return error_response(403, "Forbidden")
    ok, msg = init_db()
    if ok:
        return jsonify({"message": msg})
    return error_response(500, "Init failed", msg)


# ORM models
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), nullable=False, unique=True)
    password_hash = Column(String(255), nullable=False)  # demo: plaintext
    role = Column(String(20), nullable=False)
    full_name = Column(String(100), nullable=False)
    approved = Column(Integer, nullable=False, default=1)  # 1=approved, 0=pending
    teacher_band = Column(String(10))  # Optional: JHS or SHS for teachers
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class Student(Base):
    __tablename__ = "students"
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_number = Column(String(50), unique=True)
    first_name = Column(String(50), nullable=False)
    middle_name = Column(String(1))
    last_name = Column(String(50), nullable=False)
    date_of_birth = Column(Date)
    grade_level = Column(String(10))
    homeroom_teacher = Column(String(100))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    attendance_records = relationship("Attendance", back_populates="student")
    grades = relationship("Grade", back_populates="student")
    behaviors = relationship("BehaviorReport", back_populates="student")


class Attendance(Base):
    __tablename__ = "attendance"
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    attendance_date = Column(Date, nullable=False)
    status = Column(String(20), nullable=False)
    recorded_by = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    student = relationship("Student", back_populates="attendance_records")


class Grade(Base):
    __tablename__ = "grades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    subject = Column(String(50), nullable=False)
    assessment = Column(String(100), nullable=False)
    grade_value = Column(DECIMAL(5, 2), nullable=False)
    recorded_on = Column(Date, nullable=False)
    recorded_by = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    student = relationship("Student", back_populates="grades")


class BehaviorReport(Base):
    __tablename__ = "behaviorreports"
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    incident_date = Column(Date, nullable=False)
    severity = Column(String(20), nullable=False)
    description = Column(String(500), nullable=False)
    action_taken = Column(String(200))
    reported_by = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    student = relationship("Student", back_populates="behaviors")


class CommunicationMessage(Base):
    __tablename__ = "communications"
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(Integer, ForeignKey("students.id"))
    sender_name = Column(String(100), nullable=False)
    sender_role = Column(String(50), nullable=False)
    recipient = Column(String(100), nullable=True)
    subject = Column(String(150), nullable=False)
    message_body = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    student = relationship("Student")


class Subject(Base):
    __tablename__ = "subjects"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(150), nullable=False)
    category = Column(String(50), nullable=False)  # Core, Applied, Specialized, Institutional
    level_band = Column(String(10), nullable=False)  # JHS, SHS
    track = Column(String(50))  # e.g., STEM, ABM, HUMSS, ICT, GAS, Institutional
    grade_min = Column(Integer)  # starting grade level (7-12)
    grade_max = Column(Integer)  # ending grade level (7-12)


# Utility helpers
def error_response(status: int, message: str, detail: str = None):
    # Include detail in error message to aid debugging during local development.
    payload = {"error": message if not detail else f"{message}: {detail}"}
    if detail:
        payload["detail"] = detail
    return jsonify(payload), status


def get_session():
    try:
        session = SessionLocal()
        session.execute(text("SELECT 1"))
        return session
    except Exception as exc:  # pragma: no cover - runtime safety
        return None, exc


# Legacy MSSQL safety migrations (skip on Postgres/sqlite)
if engine.dialect.name == "mssql":
    with engine.connect() as conn:
        try:
            col_exists = conn.execute(
                text(
                    """
                    SELECT 1 FROM sys.columns 
                    WHERE Name = N'student_number' 
                      AND Object_ID = Object_ID(N'students');
                    """
                )
            ).first()
            if not col_exists:
                conn.execute(
                    text("ALTER TABLE Students ADD student_number NVARCHAR(50) NULL UNIQUE;")
                )
                conn.commit()
        except Exception:
            pass

    with engine.connect() as conn:
        try:
            col_exists = conn.execute(
                text(
                    """
                    SELECT 1 FROM sys.columns 
                    WHERE Name = N'middle_name' 
                      AND Object_ID = Object_ID(N'students');
                    """
                )
            ).first()
            if not col_exists:
                conn.execute(
                    text("ALTER TABLE Students ADD middle_name NVARCHAR(1) NULL;")
                )
                conn.commit()
        except Exception:
            pass

    with engine.connect() as conn:
        try:
            col_exists = conn.execute(
                text(
                    """
                    SELECT 1 FROM sys.columns 
                    WHERE Name = N'approved' 
                      AND Object_ID = Object_ID(N'users');
                    """
                )
            ).first()
            if not col_exists:
                conn.execute(
                    text("ALTER TABLE Users ADD approved INT NOT NULL CONSTRAINT DF_Users_Approved DEFAULT 1; UPDATE Users SET approved = 1 WHERE approved IS NULL;")
                )
                conn.commit()
        except Exception:
            pass

    with engine.connect() as conn:
        try:
            col_exists = conn.execute(
                text(
                    """
                    SELECT 1 FROM sys.columns 
                    WHERE Name = N'teacher_band' 
                      AND Object_ID = Object_ID(N'users');
                    """
                )
            ).first()
            if not col_exists:
                conn.execute(
                    text("ALTER TABLE Users ADD teacher_band NVARCHAR(10) NULL;")
                )
                conn.commit()
        except Exception:
            pass


# Simple role check using header from frontend
def require_admin():
    role = request.headers.get("X-User-Role")
    if role != "Admin":
        return error_response(403, "Admin only")
    return None


def parse_band_from_grade(grade_str: str):
    try:
        g = int(grade_str)
    except Exception:
        return None
    if 7 <= g <= 10:
        return "JHS"
    if 11 <= g <= 12:
        return "SHS"
    return None


def current_teacher_band():
    role = request.headers.get("X-User-Role")
    if role != "Teacher":
        return None
    band = request.headers.get("X-Teacher-Band")
    if band in ("JHS", "SHS"):
        return band
    return None


# Create missing tables (Communications, Subjects) without touching existing ones
try:
    Base.metadata.create_all(
        bind=engine,
        tables=[
            CommunicationMessage.__table__,
            Subject.__table__,
        ],
    )
except Exception:
    # Non-fatal; will surface on requests if missing
    pass


@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    username = data.get("username")
    password = data.get("password")
    if not username or not password:
        return error_response(400, "username and password are required")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none

    try:
        user = (
            session.query(User)
            .filter(User.username == username, User.password_hash == password)
            .first()
        )
        if not user:
            return error_response(401, "Invalid credentials")
        if user.role == "Teacher" and not user.approved:
            return error_response(403, "Account pending admin approval")
        return jsonify(
            {
                "id": user.id,
                "username": user.username,
                "role": user.role,
                "full_name": user.full_name,
                "approved": bool(user.approved),
                "teacher_band": user.teacher_band,
            }
        )
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/students", methods=["GET"])
def get_students():
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        band = current_teacher_band()
        students_query = session.query(Student)
        if band:
            students = [
                s for s in students_query.all() if parse_band_from_grade(s.grade_level) == band
            ]
        else:
            students = students_query.all()
        result = [
            {
                "id": s.id,
                "student_number": s.student_number,
                "first_name": s.first_name,
                "middle_name": s.middle_name,
                "last_name": s.last_name,
                "grade_level": s.grade_level,
                "homeroom_teacher": s.homeroom_teacher,
                "date_of_birth": s.date_of_birth.isoformat()
                if s.date_of_birth
                else None,
            }
            for s in students
        ]
        return jsonify(result)
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/students", methods=["POST"])
def create_student():
    data = request.get_json(silent=True) or {}
    required = ["first_name", "last_name", "student_number"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return error_response(400, f"Missing fields: {', '.join(missing)}")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        student = Student(
            student_number=data["student_number"].strip(),
            first_name=data["first_name"].strip(),
            middle_name=data.get("middle_name", None),
            last_name=data["last_name"].strip(),
            date_of_birth=date.fromisoformat(data["date_of_birth"])
            if data.get("date_of_birth")
            else None,
            grade_level=data.get("grade_level"),
            homeroom_teacher=data.get("homeroom_teacher"),
        )
        session.add(student)
        session.commit()
        return jsonify({"message": "Student created", "id": student.id}), 201
    except IntegrityError as exc:
        session.rollback()
        return error_response(409, "student_number must be unique", str(exc))
    except ValueError:
        session.rollback()
        return error_response(400, "date_of_birth must be YYYY-MM-DD")
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/students/<int:student_id>", methods=["PUT"])
def update_student(student_id: int):
    data = request.get_json(silent=True) or {}
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        student = session.query(Student).filter_by(id=student_id).first()
        if not student:
            return error_response(404, "Student not found")
        for field in ["first_name", "last_name", "grade_level", "homeroom_teacher", "student_number"]:
            if field in data:
                setattr(student, field, data[field])
        if "middle_name" in data:
            student.middle_name = data["middle_name"]
        if "date_of_birth" in data:
            if data["date_of_birth"]:
                try:
                    student.date_of_birth = date.fromisoformat(data["date_of_birth"])
                except ValueError:
                    return error_response(400, "date_of_birth must be YYYY-MM-DD")
            else:
                student.date_of_birth = None
        session.commit()
        return jsonify({"message": "Student updated"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/students/<int:student_id>", methods=["DELETE"])
def delete_student(student_id: int):
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        student = session.query(Student).filter_by(id=student_id).first()
        if not student:
            return error_response(404, "Student not found")
        # Clean up dependent records to satisfy FK constraints
        session.query(Grade).filter_by(student_id=student_id).delete(synchronize_session=False)
        session.query(Attendance).filter_by(student_id=student_id).delete(synchronize_session=False)
        session.query(BehaviorReport).filter_by(student_id=student_id).delete(synchronize_session=False)
        session.query(CommunicationMessage).filter_by(student_id=student_id).delete(synchronize_session=False)
        session.delete(student)
        session.commit()
        return jsonify({"message": "Student deleted"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/grades", methods=["POST"])
def add_grade():
    data = request.get_json(silent=True) or {}
    required_fields = ["student_id", "subject", "assessment", "grade_value"]
    missing = [f for f in required_fields if f not in data]
    if missing:
        return error_response(400, f"Missing fields: {', '.join(missing)}")

    recorded_on = data.get("recorded_on")
    try:
        recorded_date = (
            date.fromisoformat(recorded_on) if recorded_on else date.today()
        )
    except ValueError:
        return error_response(400, "recorded_on must be YYYY-MM-DD")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        # Ensure student exists
        student = session.query(Student).filter_by(id=data["student_id"]).first()
        if not student:
            return error_response(404, "Student not found")

        grade = Grade(
            student_id=data["student_id"],
            subject=data["subject"],
            assessment=data["assessment"],
            grade_value=data["grade_value"],
            recorded_on=recorded_date,
            recorded_by=data.get("recorded_by"),
        )
        session.add(grade)
        session.commit()
        return jsonify({"message": "Grade recorded", "id": grade.id}), 201
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/grades", methods=["GET"])
def list_grades():
    student_id = request.args.get("student_id", type=int)
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        band = current_teacher_band()
        query = session.query(Grade)
        if student_id:
            query = query.filter(Grade.student_id == student_id)
        if band:
            # Filter by student band
            grades = []
            for g in query.order_by(Grade.recorded_on.desc()).all():
                st = session.query(Student).filter_by(id=g.student_id).first()
                if st and parse_band_from_grade(st.grade_level) == band:
                    grades.append(g)
        else:
            grades = query.order_by(Grade.recorded_on.desc()).all()
        return jsonify(
            [
                {
                    "id": g.id,
                    "student_id": g.student_id,
                    "subject": g.subject,
                    "assessment": g.assessment,
                    "grade_value": float(g.grade_value),
                    "recorded_on": g.recorded_on.isoformat(),
                    "recorded_by": g.recorded_by,
                }
                for g in grades
            ]
        )
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/grades/<int:grade_id>", methods=["PUT"])
def update_grade(grade_id: int):
    data = request.get_json(silent=True) or {}
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        grade = session.query(Grade).filter_by(id=grade_id).first()
        if not grade:
            return error_response(404, "Grade not found")
        for field in ["subject", "assessment", "grade_value", "recorded_by"]:
            if field in data:
                setattr(grade, field, data[field])
        if "recorded_on" in data:
            try:
                grade.recorded_on = date.fromisoformat(data["recorded_on"])
            except ValueError:
                return error_response(400, "recorded_on must be YYYY-MM-DD")
        session.commit()
        return jsonify({"message": "Grade updated"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/grades/<int:grade_id>", methods=["DELETE"])
def delete_grade(grade_id: int):
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        grade = session.query(Grade).filter_by(id=grade_id).first()
        if not grade:
            return error_response(404, "Grade not found")
        session.delete(grade)
        session.commit()
        return jsonify({"message": "Grade deleted"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/dashboard-stats", methods=["GET"])
def dashboard_stats():
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        # Attendance distribution
        attendance_rows = (
            session.query(Attendance.status, func.count().label("count"))
            .group_by(Attendance.status)
            .all()
        )
        attendance = {"Present": 0, "Absent": 0, "Tardy": 0}
        for status, count in attendance_rows:
            attendance[status] = count

        # Average grades per subject
        grade_rows = (
            session.query(Grade.subject, func.avg(Grade.grade_value).label("avg_grade"))
            .group_by(Grade.subject)
            .all()
        )
        averages = [{"subject": r[0], "average": float(r[1])} for r in grade_rows]

        totals = {
            "students": session.query(func.count(Student.id)).scalar(),
            "grades": session.query(func.count(Grade.id)).scalar(),
            "behaviors": session.query(func.count(BehaviorReport.id)).scalar(),
            "communications": session.query(func.count(CommunicationMessage.id)).scalar(),
        }

        return jsonify(
            {"attendance": attendance, "average_grades": averages, "totals": totals}
        )
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/communications", methods=["GET"])
def list_communications():
    student_id = request.args.get("student_id", type=int)
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        query = (
            session.query(
                CommunicationMessage,
                Student.first_name,
                Student.last_name,
            )
            .outerjoin(Student, CommunicationMessage.student_id == Student.id)
        )
        if student_id:
            query = query.filter(CommunicationMessage.student_id == student_id)
        messages = (
            query.order_by(CommunicationMessage.created_at.desc())
            .all()
        )
        return jsonify(
            [
                {
                    "id": msg.id,
                    "student_id": msg.student_id,
                    "student_name": f"{fn} {ln}".strip() if fn or ln else None,
                    "sender_name": msg.sender_name,
                    "sender_role": msg.sender_role,
                    "recipient": msg.recipient,
                    "subject": msg.subject,
                    "message_body": msg.message_body,
                    "created_at": msg.created_at.isoformat(),
                }
                for (msg, fn, ln) in messages
            ]
        )
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/communications", methods=["POST"])
def create_communication():
    data = request.get_json(silent=True) or {}
    required = ["sender_name", "sender_role", "subject", "message_body"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return error_response(400, f"Missing fields: {', '.join(missing)}")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        # Optional student check
        if data.get("student_id"):
            exists = session.query(Student.id).filter_by(id=data["student_id"]).first()
            if not exists:
                return error_response(404, "Student not found for communication")
        message = CommunicationMessage(
            student_id=data.get("student_id"),
            sender_name=data["sender_name"],
            sender_role=data["sender_role"],
            recipient=data.get("recipient"),
            subject=data["subject"],
            message_body=data["message_body"],
        )
        session.add(message)
        session.commit()
        return jsonify({"message": "Communication logged", "id": message.id}), 201
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/users", methods=["GET"])
def list_users():
    role = request.args.get("role")
    user_id = request.args.get("user_id", type=int)
    pending_only = request.args.get("pending", type=int)
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        query = session.query(User.id, User.username, User.full_name, User.role, User.approved, User.teacher_band)
        if user_id:
            query = query.filter(User.id == user_id)
        if role:
            query = query.filter(User.role == role)
        if pending_only:
            query = query.filter(User.approved == 0)
        rows = query.order_by(User.full_name.asc()).all()
        return jsonify(
            [
                {
                    "id": r.id,
                    "username": r.username,
                    "full_name": r.full_name,
                    "role": r.role,
                    "approved": bool(r.approved),
                    "teacher_band": r.teacher_band,
                }
                for r in rows
            ]
        )
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/users", methods=["POST"])
def create_user():
    data = request.get_json(silent=True) or {}
    required = ["username", "password", "role", "full_name"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return error_response(400, f"Missing fields: {', '.join(missing)}")
    if data["role"] not in ("Admin", "Teacher", "Parent"):
        return error_response(400, "role must be Admin, Teacher, or Parent")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        exists = session.query(User).filter_by(username=data["username"]).first()
        if exists:
            return error_response(409, "Username already exists")
        user = User(
            username=data["username"],
            password_hash=data["password"],  # plaintext for demo
            role=data["role"],
            full_name=data["full_name"],
            approved=1,
            teacher_band=data.get("teacher_band"),
        )
        session.add(user)
        session.commit()
        return jsonify({"message": "User created", "id": user.id}), 201
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/users/<int:user_id>", methods=["PUT"])
def update_user(user_id: int):
    data = request.get_json(silent=True) or {}
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        user = session.query(User).filter_by(id=user_id).first()
        if not user:
            return error_response(404, "User not found")
        # Only Admin can approve
        if "approved" in data:
            admin_err = require_admin()
            if admin_err:
                return admin_err
        if "role" in data:
            if data["role"] not in ("Admin", "Teacher", "Parent"):
                return error_response(400, "role must be Admin, Teacher, or Parent")
            user.role = data["role"]
        if "full_name" in data and data["full_name"]:
            user.full_name = data["full_name"]
        if "password" in data and data["password"]:
            user.password_hash = data["password"]  # plaintext for demo
        if "approved" in data:
            user.approved = 1 if data["approved"] else 0
        if "teacher_band" in data:
            user.teacher_band = data["teacher_band"]
        session.commit()
        return jsonify({"message": "User updated"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/users/<int:user_id>", methods=["DELETE"])
def delete_user(user_id: int):
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        user = session.query(User).filter_by(id=user_id).first()
        if not user:
            return error_response(404, "User not found")
        session.delete(user)
        session.commit()
        return jsonify({"message": "User deleted"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/signup/teacher", methods=["POST"])
def signup_teacher():
    data = request.get_json(silent=True) or {}
    required = ["username", "password", "full_name"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return error_response(400, f"Missing fields: {', '.join(missing)}")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        exists = session.query(User).filter_by(username=data["username"]).first()
        if exists:
            return error_response(409, "Username already exists")
        user = User(
            username=data["username"].strip(),
            password_hash=data["password"],
            role="Teacher",
            full_name=data["full_name"].strip(),
            approved=0,
            teacher_band=data.get("teacher_band"),
        )
        session.add(user)
        session.commit()
        return jsonify({"message": "Signup submitted. Await admin approval.", "id": user.id}), 201
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()

@app.route("/api/signup/parent", methods=["POST"])
def parent_signup():
    data = request.get_json(silent=True) or {}
    required = ["username", "password", "full_name", "student_number"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return error_response(400, f"Missing fields: {', '.join(missing)}")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        student = (
            session.query(Student)
            .filter(Student.student_number == data["student_number"])
            .first()
        )
        if not student:
            return error_response(404, "Student number not found")

        exists = session.query(User).filter_by(username=data["username"]).first()
        if exists:
            return error_response(409, "Username already exists")

        user = User(
            username=data["username"],
            password_hash=data["password"],  # plaintext for demo
            role="Parent",
            full_name=data["full_name"],
        )
        session.add(user)
        session.commit()
        return jsonify({"message": "Parent account created", "id": user.id}), 201
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/adviser-insights", methods=["GET"])
def adviser_insights():
    """
    Provides quick insights for advisers/program heads:
    - lowest average grades (top 5)
    - attendance risk (lowest present rates, top 5)
    """
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        # Lowest averages
        grade_avgs = (
            session.query(
                Student.id,
                Student.first_name,
                Student.last_name,
                func.avg(Grade.grade_value).label("avg_grade"),
            )
            .join(Grade, Grade.student_id == Student.id)
            .group_by(Student.id, Student.first_name, Student.last_name)
            .order_by(text("avg_grade ASC"))
            .limit(5)
            .all()
        )
        low_grades = [
            {
                "student_id": sid,
                "student_name": f"{fn} {ln}".strip(),
                "average": float(avg),
            }
            for sid, fn, ln, avg in grade_avgs
        ]

        # Attendance risk: present ratio
        attn_subq = (
            session.query(
                Student.id.label("sid"),
                Student.first_name.label("fn"),
                Student.last_name.label("ln"),
                func.sum(case((Attendance.status == "Present", 1), else_=0)).label(
                    "present_count"
                ),
                func.count(Attendance.id).label("total_count"),
            )
            .join(Attendance, Attendance.student_id == Student.id)
            .group_by(Student.id, Student.first_name, Student.last_name)
            .having(func.count(Attendance.id) > 0)
            .subquery()
        )

        attn = (
            session.query(
                attn_subq.c.sid,
                attn_subq.c.fn,
                attn_subq.c.ln,
                attn_subq.c.present_count,
                attn_subq.c.total_count,
            )
            .order_by(
                (attn_subq.c.present_count * 1.0)
                / func.nullif(attn_subq.c.total_count, 0)
            )
            .limit(5)
            .all()
        )
        attendance_risk = []
        for sid, fn, ln, present, total in attn:
            rate = float(present) / float(total) if total else 0.0
            attendance_risk.append(
                {
                    "student_id": sid,
                    "student_name": f"{fn} {ln}".strip(),
                    "present_rate": round(rate * 100, 2),
                    "total_logs": int(total),
                }
            )

        return jsonify({"low_grades": low_grades, "attendance_risk": attendance_risk})
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/subjects", methods=["GET"])
def list_subjects():
    level_band = request.args.get("level_band")
    track = request.args.get("track")
    category = request.args.get("category")
    grade = request.args.get("grade", type=int)

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        band_header = current_teacher_band()
        query = session.query(Subject)
        if level_band:
            query = query.filter(Subject.level_band == level_band)
        if band_header:
            query = query.filter(Subject.level_band == band_header)
        if track:
            query = query.filter(Subject.track == track)
        if category:
            query = query.filter(Subject.category == category)
        if grade:
            query = query.filter(
                and_(
                    or_(Subject.grade_min == None, Subject.grade_min <= grade),  # noqa: E711
                    or_(Subject.grade_max == None, Subject.grade_max >= grade),  # noqa: E711
                )
            )
        subjects = query.order_by(Subject.level_band, Subject.category, Subject.track, Subject.name).all()
        return jsonify(
            [
                {
                    "id": s.id,
                    "name": s.name,
                    "category": s.category,
                    "level_band": s.level_band,
                    "track": s.track,
                    "grade_min": s.grade_min,
                    "grade_max": s.grade_max,
                }
                for s in subjects
            ]
        )
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/subjects", methods=["POST"])
def create_subject():
    admin_err = require_admin()
    if admin_err:
        return admin_err
    data = request.get_json(silent=True) or {}
    required = ["name", "category", "level_band"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return error_response(400, f"Missing fields: {', '.join(missing)}")

    level_band = data["level_band"]
    if level_band not in ("JHS", "SHS"):
        return error_response(400, "level_band must be JHS or SHS")
    category = data["category"]
    allowed_cats = ("Core", "Applied", "Specialized", "Institutional")
    if category not in allowed_cats:
        return error_response(400, f"category must be one of {', '.join(allowed_cats)}")

    grade_min = data.get("grade_min")
    grade_max = data.get("grade_max")
    if grade_min is not None and (not isinstance(grade_min, int) or grade_min < 7 or grade_min > 12):
        return error_response(400, "grade_min must be 7-12")
    if grade_max is not None and (not isinstance(grade_max, int) or grade_max < 7 or grade_max > 12):
        return error_response(400, "grade_max must be 7-12")
    if grade_min and grade_max and grade_min > grade_max:
        return error_response(400, "grade_min cannot exceed grade_max")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        subject = Subject(
            name=data["name"].strip(),
            category=category,
            level_band=level_band,
            track=data.get("track"),
            grade_min=grade_min,
            grade_max=grade_max,
        )
        session.add(subject)
        session.commit()
        return jsonify({"message": "Subject created", "id": subject.id}), 201
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/subjects/<int:subject_id>", methods=["PUT"])
def update_subject(subject_id: int):
    admin_err = require_admin()
    if admin_err:
        return admin_err
    data = request.get_json(silent=True) or {}

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        subject = session.query(Subject).filter_by(id=subject_id).first()
        if not subject:
            return error_response(404, "Subject not found")

        if "name" in data:
            subject.name = data["name"].strip()
        if "category" in data:
            allowed_cats = ("Core", "Applied", "Specialized", "Institutional")
            if data["category"] not in allowed_cats:
                return error_response(400, f"category must be one of {', '.join(allowed_cats)}")
            subject.category = data["category"]
        if "level_band" in data:
            if data["level_band"] not in ("JHS", "SHS"):
                return error_response(400, "level_band must be JHS or SHS")
            subject.level_band = data["level_band"]
        if "track" in data:
            subject.track = data["track"]
        for fld in ("grade_min", "grade_max"):
            if fld in data:
                val = data[fld]
                if val is not None and (not isinstance(val, int) or val < 7 or val > 12):
                    return error_response(400, f"{fld} must be 7-12")
                setattr(subject, fld, val)
        if subject.grade_min and subject.grade_max and subject.grade_min > subject.grade_max:
            return error_response(400, "grade_min cannot exceed grade_max")
        session.commit()
        return jsonify({"message": "Subject updated"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/subjects/<int:subject_id>", methods=["DELETE"])
def delete_subject(subject_id: int):
    admin_err = require_admin()
    if admin_err:
        return admin_err
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        subject = session.query(Subject).filter_by(id=subject_id).first()
        if not subject:
            return error_response(404, "Subject not found")
        session.delete(subject)
        session.commit()
        return jsonify({"message": "Subject deleted"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/attendance", methods=["GET"])
def list_attendance():
    student_id = request.args.get("student_id", type=int)
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        band = current_teacher_band()
        query = session.query(Attendance)
        if student_id:
            query = query.filter(Attendance.student_id == student_id)
        if band:
            records = []
            for r in query.order_by(Attendance.attendance_date.desc()).all():
                st = session.query(Student).filter_by(id=r.student_id).first()
                if st and parse_band_from_grade(st.grade_level) == band:
                    records.append(r)
        else:
            records = query.order_by(Attendance.attendance_date.desc()).all()
        return jsonify(
            [
                {
                    "id": a.id,
                    "student_id": a.student_id,
                    "attendance_date": a.attendance_date.isoformat(),
                    "status": a.status,
                    "recorded_by": a.recorded_by,
                }
                for a in records
            ]
        )
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/attendance", methods=["POST"])
def create_attendance():
    data = request.get_json(silent=True) or {}
    required = ["student_id", "attendance_date", "status"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return error_response(400, f"Missing fields: {', '.join(missing)}")
    try:
        attendance_date = date.fromisoformat(data["attendance_date"])
    except ValueError:
        return error_response(400, "attendance_date must be YYYY-MM-DD")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        student = session.query(Student).filter_by(id=data["student_id"]).first()
        if not student:
            return error_response(404, "Student not found")
        record = Attendance(
            student_id=data["student_id"],
            attendance_date=attendance_date,
            status=data["status"],
            recorded_by=data.get("recorded_by"),
        )
        session.add(record)
        session.commit()
        return jsonify({"message": "Attendance recorded", "id": record.id}), 201
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/attendance/<int:attendance_id>", methods=["PUT"])
def update_attendance(attendance_id: int):
    data = request.get_json(silent=True) or {}
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        record = session.query(Attendance).filter_by(id=attendance_id).first()
        if not record:
            return error_response(404, "Attendance not found")
        if "attendance_date" in data:
            try:
                record.attendance_date = date.fromisoformat(data["attendance_date"])
            except ValueError:
                return error_response(400, "attendance_date must be YYYY-MM-DD")
        for field in ["status", "recorded_by", "student_id"]:
            if field in data:
                setattr(record, field, data[field])
        session.commit()
        return jsonify({"message": "Attendance updated"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/attendance/<int:attendance_id>", methods=["DELETE"])
def delete_attendance(attendance_id: int):
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        record = session.query(Attendance).filter_by(id=attendance_id).first()
        if not record:
            return error_response(404, "Attendance not found")
        session.delete(record)
        session.commit()
        return jsonify({"message": "Attendance deleted"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.errorhandler(404)
def not_found(_):
    return error_response(404, "Not found")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

