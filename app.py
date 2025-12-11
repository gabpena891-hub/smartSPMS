import os
import logging
import re
import io
from datetime import date, datetime
from werkzeug.security import generate_password_hash, check_password_hash

from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS
from sqlalchemy import (Column, Date, DateTime, DECIMAL, ForeignKey, Integer,
                        String, Text, Float, create_engine, func, text, case, and_, or_)
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
logging.basicConfig(level=logging.INFO)

def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        return True, "tables ensured"
    except Exception as exc:
        logging.warning("DB init failed: %s", exc)
        return False, str(exc)


def hash_password(raw: str):
    if not raw:
        return None
    return generate_password_hash(raw)


def verify_password(stored: str, provided: str) -> bool:
    """
    Supports legacy plaintext (stored == provided) and hashed (Werkzeug).
    """
    if not stored or not provided:
        return False
    if stored == provided:
        return True
    try:
        return check_password_hash(stored, provided)
    except Exception:
        return False


# Ensure tables exist (idempotent; safe for first run on Render/sqlite)
init_db()
with app.app_context():
    init_db()


def ensure_section_schema():
    """
    Best-effort: add students.section_id and student_subjects table if missing.
    This runs at startup to avoid crashes when the column/table are absent.
    """
    if engine.dialect.name == "postgresql":
        ddl = """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='students' AND column_name='section_id') THEN
                ALTER TABLE students ADD COLUMN section_id INTEGER REFERENCES sections(id) ON DELETE SET NULL;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='student_subjects') THEN
                CREATE TABLE student_subjects (
                    id SERIAL PRIMARY KEY,
                    student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
                    subject_id INTEGER NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
                    teacher_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    section_id INTEGER REFERENCES sections(id) ON DELETE SET NULL,
                    term VARCHAR(20),
                    active INT DEFAULT 1,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
                );
            END IF;
        END $$;
        """
    else:
        ddls = [
            "ALTER TABLE students ADD COLUMN section_id INTEGER;",
            """
            CREATE TABLE IF NOT EXISTS student_subjects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id INTEGER NOT NULL,
                subject_id INTEGER NOT NULL,
                teacher_id INTEGER,
                section_id INTEGER,
                term VARCHAR(20),
                active INT DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """,
        ]
    try:
        with engine.begin() as conn:
            if engine.dialect.name == "postgresql":
                conn.execute(text(ddl))
            else:
                for stmt in ddls:
                    try:
                        conn.execute(text(stmt))
                    except Exception:
                        pass
    except Exception as exc:
        logging.warning("ensure_section_schema failed: %s", exc)


def ensure_attendance_schema():
    """
    Best-effort: add attendance.section_id and attendance.subject_id if missing.
    """
    if engine.dialect.name == "postgresql":
        ddl = """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='attendance' AND column_name='section_id') THEN
                ALTER TABLE attendance ADD COLUMN section_id INTEGER REFERENCES sections(id) ON DELETE SET NULL;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='attendance' AND column_name='subject_id') THEN
                ALTER TABLE attendance ADD COLUMN subject_id INTEGER REFERENCES subjects(id) ON DELETE SET NULL;
            END IF;
        END $$;
        """
    else:
        ddls = [
            "ALTER TABLE attendance ADD COLUMN section_id INTEGER;",
            "ALTER TABLE attendance ADD COLUMN subject_id INTEGER;",
        ]
    try:
        with engine.begin() as conn:
            if engine.dialect.name == "postgresql":
                conn.execute(text(ddl))
            else:
                for stmt in ddls:
                    try:
                        conn.execute(text(stmt))
                    except Exception:
                        pass
    except Exception as exc:
        logging.warning("ensure_attendance_schema failed: %s", exc)


def ensure_communications_schema():
    """
    Best-effort: add communications columns if missing.
    """
    if engine.dialect.name == "postgresql":
        ddl = """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='communications' AND column_name='student_id') THEN
                ALTER TABLE communications ADD COLUMN student_id INTEGER REFERENCES students(id) ON DELETE SET NULL;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='communications' AND column_name='sender_name') THEN
                ALTER TABLE communications ADD COLUMN sender_name VARCHAR(100);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='communications' AND column_name='sender_role') THEN
                ALTER TABLE communications ADD COLUMN sender_role VARCHAR(50);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='communications' AND column_name='recipient') THEN
                ALTER TABLE communications ADD COLUMN recipient VARCHAR(100);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='communications' AND column_name='subject') THEN
                ALTER TABLE communications ADD COLUMN subject VARCHAR(150);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='communications' AND column_name='message_body') THEN
                ALTER TABLE communications ADD COLUMN message_body TEXT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='communications' AND column_name='created_at') THEN
                ALTER TABLE communications ADD COLUMN created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW();
            END IF;
        END $$;
        """
    else:
        ddls = [
            "ALTER TABLE communications ADD COLUMN student_id INTEGER;",
            "ALTER TABLE communications ADD COLUMN sender_name VARCHAR(100);",
            "ALTER TABLE communications ADD COLUMN sender_role VARCHAR(50);",
            "ALTER TABLE communications ADD COLUMN recipient VARCHAR(100);",
            "ALTER TABLE communications ADD COLUMN subject VARCHAR(150);",
            "ALTER TABLE communications ADD COLUMN message_body TEXT;",
            "ALTER TABLE communications ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP;",
        ]
    try:
        with engine.begin() as conn:
            if engine.dialect.name == "postgresql":
                conn.execute(text(ddl))
            else:
                for stmt in ddls:
                    try:
                        conn.execute(text(stmt))
                    except Exception:
                        pass
    except Exception as exc:
        logging.warning("ensure_communications_schema failed: %s", exc)


ensure_communications_schema()


def ensure_schedule_schema():
    """Best-effort creation of rooms and schedules tables."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS rooms (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name VARCHAR(100) UNIQUE NOT NULL,
                        building VARCHAR(100),
                        level VARCHAR(50),
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                    if engine.dialect.name != "postgresql"
                    else """
                    CREATE TABLE IF NOT EXISTS rooms (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) UNIQUE NOT NULL,
                        building VARCHAR(100),
                        level VARCHAR(50),
                        created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
                    );
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS schedules (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        section_id INTEGER NOT NULL,
                        subject_id INTEGER NOT NULL,
                        teacher_id INTEGER,
                        room_id INTEGER,
                        day_of_week INTEGER NOT NULL,
                        start_time VARCHAR(5) NOT NULL,
                        end_time VARCHAR(5) NOT NULL,
                        notes VARCHAR(200),
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                    if engine.dialect.name != "postgresql"
                    else """
                    CREATE TABLE IF NOT EXISTS schedules (
                        id SERIAL PRIMARY KEY,
                        section_id INTEGER NOT NULL,
                        subject_id INTEGER NOT NULL,
                        teacher_id INTEGER,
                        room_id INTEGER,
                        day_of_week INTEGER NOT NULL,
                        start_time VARCHAR(5) NOT NULL,
                        end_time VARCHAR(5) NOT NULL,
                        notes VARCHAR(200),
                        created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
                    );
                    """
                )
            )
    except Exception as exc:
        logging.warning("ensure_schedule_schema failed: %s", exc)


ensure_schedule_schema()


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


@app.route("/api/admin/seed-admin", methods=["POST", "GET"])
def admin_seed():
    # Protect with ADMIN_INIT_TOKEN if set
    token = os.environ.get("ADMIN_INIT_TOKEN")
    if token:
        provided = request.headers.get("X-Admin-Init-Token") or request.args.get("token")
        if provided != token:
            return error_response(403, "Forbidden")

    username = "gabpena891@gmail.com"
    password = "chin1979"
    full_name = "Admin User"

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        exists = session.query(User).filter_by(username=username).first()
        if exists:
            return jsonify({"message": "Admin already exists"})
        user = User(
            username=username,
            password_hash=hash_password(password),
            role="Admin",
            full_name=full_name,
            approved=1,
            teacher_band=None,
        )
        session.add(user)
        session.commit()
        ensure_subjects_catalog()
        return jsonify({"message": "Admin seeded"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/schedule/pdf", methods=["GET"])
def schedule_pdf():
    section_id = request.args.get("section_id", type=int)
    teacher_id = request.args.get("teacher_id", type=int)
    if not section_id and not teacher_id:
        return error_response(400, "section_id or teacher_id is required")
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        q = session.query(ScheduleEntry)
        title = "Schedule"
        if section_id:
            q = q.filter(ScheduleEntry.section_id == section_id)
            sec = session.query(Section).filter_by(id=section_id).first()
            if not sec:
                return error_response(404, "Section not found")
            title = f"Section Schedule - {sec.name}"
        if teacher_id:
            q = q.filter(ScheduleEntry.teacher_id == teacher_id)
            teacher = session.query(User).filter_by(id=teacher_id).first()
            if not teacher:
                return error_response(404, "Teacher not found")
            title = f"Teacher Schedule - {teacher.full_name or teacher.username}"
        rows = q.all()
        data = []
        for r in rows:
            subj = session.query(Subject).filter_by(id=r.subject_id).first()
            sec = session.query(Section).filter_by(id=r.section_id).first()
            teacher = session.query(User).filter_by(id=r.teacher_id).first() if r.teacher_id else None
            room = session.query(Room).filter_by(id=r.room_id).first() if r.room_id else None
            data.append(
                {
                    "day_of_week": r.day_of_week,
                    "start_time": r.start_time,
                    "end_time": r.end_time,
                    "subject_name": subj.name if subj else None,
                    "section_name": sec.name if sec else None,
                    "teacher_name": teacher.full_name if teacher else None,
                    "room_name": room.name if room else None,
                }
            )
        pdf_buf = make_schedule_pdf(data, title=title)
        return send_file(
            pdf_buf,
            mimetype="application/pdf",
            as_attachment=True,
            download_name=f"{title.replace(' ', '_')}.pdf",
        )
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


def seed_subjects_data(session):
    session.query(Subject).delete()

    def add_subjects(names, band, category, ww, pt, qa, gmin=None, gmax=None):
        for n in names:
            session.add(
                Subject(
                    name=n,
                    category=category,
                    level_band=band,
                    grade_min=gmin,
                    grade_max=gmax,
                    weight_ww=ww,
                    weight_pt=pt,
                    weight_qa=qa,
                )
            )

    # JHS per-grade subjects (DepEd-aligned naming per grade 7-10)
    for g in range(7, 11):
        # Languages / AP / EsP
        add_subjects(
            [
                f"Filipino {g}",
                f"English {g}",
                f"Araling Panlipunan {g}",
                f"Edukasyon sa Pagpapakatao {g}",
            ],
            "JHS",
            "Core",
            0.30,
            0.50,
            0.20,
            g,
            g,
        )
        # Math & Science
        add_subjects(
            [
                f"Mathematics {g}",
                f"Science {g}",
            ],
            "JHS",
            "Core",
            0.40,
            0.40,
            0.20,
            g,
            g,
        )
        # MAPEH & TLE
        add_subjects(
            [
                f"MAPEH {g}",
                f"TLE {g}",
            ],
            "JHS",
            "Core",
            0.20,
            0.60,
            0.20,
            g,
            g,
        )

    # Group D: SHS Core (WW=0.25, PT=0.50, QA=0.25)
    add_subjects(
        [
            "Oral Communication",
            "Reading and Writing",
            "Komunikasyon at Pananaliksik",
            "General Mathematics",
            "Statistics and Probability",
            "Earth and Life Science",
            "Physical Education and Health",
            "Understanding Culture, Society, and Politics",
        ],
        "SHS",
        "Core",
        0.25,
        0.50,
        0.25,
        11,
        12,
    )

    # Group E: SHS Applied/Specialized (WW=0.25, PT=0.45, QA=0.30)
    add_subjects(
        [
            "Empowerment Technologies",
            "Entrepreneurship",
            "Practical Research 1",
            "Practical Research 2",
            "Inquiries, Investigations, and Immersion",
        ],
        "SHS",
        "Applied",
        0.25,
        0.45,
        0.30,
        11,
        12,
    )


def ensure_subjects_catalog():
    """Seed default subjects if none exist to keep scheduling/auto-assign working."""
    session = SessionLocal()
    try:
        total = session.query(Subject).count()
        if total == 0:
            seed_subjects_data(session)
            session.commit()
    except Exception as exc:
        logging.warning("ensure_subjects_catalog failed: %s", exc)
    finally:
        session.close()


@app.route("/api/admin/seed-subjects", methods=["POST", "GET"])
def admin_seed_subjects():
    # Protect with ADMIN_INIT_TOKEN if set
    token = os.environ.get("ADMIN_INIT_TOKEN")
    if token:
        provided = request.headers.get("X-Admin-Init-Token") or request.args.get("token")
        if provided != token:
            return error_response(403, "Forbidden")

    ok, msg = init_db()
    if not ok:
        return error_response(500, "Init failed", msg)

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        seed_subjects_data(session)
        session.commit()
        return jsonify({"message": "Subjects seeded"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/admin/patch-subject-weights", methods=["POST", "GET"])
def admin_patch_subject_weights():
    """Add weight_ww/weight_pt/weight_qa columns if missing (for Postgres without shell)."""
    token = os.environ.get("ADMIN_INIT_TOKEN")
    if token:
        provided = request.headers.get("X-Admin-Init-Token") or request.args.get("token")
        if provided != token:
            return error_response(403, "Forbidden")

    ddl = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='subjects' AND column_name='weight_ww') THEN
            ALTER TABLE subjects ADD COLUMN weight_ww FLOAT DEFAULT 0;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='subjects' AND column_name='weight_pt') THEN
            ALTER TABLE subjects ADD COLUMN weight_pt FLOAT DEFAULT 0;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='subjects' AND column_name='weight_qa') THEN
            ALTER TABLE subjects ADD COLUMN weight_qa FLOAT DEFAULT 0;
        END IF;
    END $$;
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
        return jsonify({"message": "Subject weight columns ensured"})
    except Exception as exc:
        return error_response(500, "Patch failed", str(exc))


@app.route("/api/admin/patch-subjects-teacher", methods=["GET"])
def patch_subjects_teacher():
    # Protect with ADMIN_INIT_TOKEN if set
    token = os.environ.get("ADMIN_INIT_TOKEN")
    if token:
        provided = request.headers.get("X-Admin-Init-Token") or request.args.get("token")
        if provided != token:
            return error_response(403, "Forbidden")
    try:
        with engine.begin() as conn:
            check_sql = text("SELECT column_name FROM information_schema.columns WHERE table_name='subjects' AND column_name='teacher_id'")
            exists = conn.execute(check_sql).scalar()
            if not exists:
                conn.execute(text("ALTER TABLE subjects ADD COLUMN teacher_id INTEGER"))
                return jsonify({"message": "Added teacher_id to subjects table"})
            else:
                return jsonify({"message": "teacher_id already exists"})
    except Exception as e:
        return error_response(500, str(e))


@app.route("/api/admin/patch-grades-schema", methods=["POST", "GET"])
def admin_patch_grades_schema():
    """Add raw_score, max_score, component columns if missing."""
    token = os.environ.get("ADMIN_INIT_TOKEN")
    if token:
        provided = request.headers.get("X-Admin-Init-Token") or request.args.get("token")
        if provided != token:
            return error_response(403, "Forbidden")

    ddl = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='grades' AND column_name='raw_score') THEN
            ALTER TABLE grades ADD COLUMN raw_score INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='grades' AND column_name='max_score') THEN
            ALTER TABLE grades ADD COLUMN max_score INTEGER;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='grades' AND column_name='component') THEN
            ALTER TABLE grades ADD COLUMN component VARCHAR(5);
        END IF;
    END $$;
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
        return jsonify({"message": "Grades schema patched"})
    except Exception as exc:
        return error_response(500, "Patch failed", str(exc))


@app.route("/api/admin/migrate-uppercase", methods=["POST", "GET"])
def admin_migrate_uppercase():
    """
    One-time helper: copies data from quoted/uppercase tables ("Users", "Students")
    into the lowercase tables (users, students) used by the app. Only copies if
    the lowercase table is empty. Protect with ADMIN_INIT_TOKEN if set.
    """
    token = os.environ.get("ADMIN_INIT_TOKEN")
    if token:
        provided = request.headers.get("X-Admin-Init-Token") or request.args.get("token")
        if provided != token:
            return error_response(403, "Forbidden")

    try:
        with engine.begin() as conn:
            # Users
            lower_users_count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
            upper_users_exists = conn.execute(
                text("SELECT to_regclass('\"Users\"') is not null")
            ).scalar()
            migrated_users = 0
            if lower_users_count == 0 and upper_users_exists:
                rows = conn.execute(text('SELECT username, password_hash, role, full_name, approved, teacher_band, created_at FROM "Users"')).fetchall()
                for r in rows:
                    conn.execute(
                        text(
                            "INSERT INTO users (username, password_hash, role, full_name, approved, teacher_band, created_at) "
                            "VALUES (:u,:p,:r,:f,:a,:b,:c)"
                        ),
                        {"u": r[0], "p": r[1], "r": r[2], "f": r[3], "a": r[4], "b": r[5], "c": r[6]},
                    )
                    migrated_users += 1

            # Students
            lower_students_count = conn.execute(text("SELECT COUNT(*) FROM students")).scalar()
            upper_students_exists = conn.execute(
                text("SELECT to_regclass('\"Students\"') is not null")
            ).scalar()
            migrated_students = 0
            if lower_students_count == 0 and upper_students_exists:
                rows = conn.execute(
                    text('SELECT student_number, first_name, middle_name, last_name, date_of_birth, grade_level, homeroom_teacher, created_at FROM "Students"')
                ).fetchall()
                for r in rows:
                    conn.execute(
                        text(
                            "INSERT INTO students (student_number, first_name, middle_name, last_name, date_of_birth, grade_level, homeroom_teacher, created_at) "
                            "VALUES (:sn,:fn,:mn,:ln,:dob,:gl,:hr,:ca)"
                        ),
                        {
                            "sn": r[0],
                            "fn": r[1],
                            "mn": r[2],
                            "ln": r[3],
                            "dob": r[4],
                            "gl": r[5],
                            "hr": r[6],
                            "ca": r[7],
                        },
                    )
                    migrated_students += 1

        return jsonify({"message": "Migration complete", "users_migrated": migrated_users, "students_migrated": migrated_students})
    except Exception as exc:
        return error_response(500, "Migration failed", str(exc))


@app.route("/api/admin/force-migrate-uppercase", methods=["POST", "GET"])
def admin_force_migrate_uppercase():
    """
    Force copy from quoted uppercase tables ("Users", "Students") into lowercase
    tables, even if lowercase already has data. Uses ON CONFLICT DO NOTHING to
    avoid duplicates. Protect with ADMIN_INIT_TOKEN if set.
    """
    token = os.environ.get("ADMIN_INIT_TOKEN")
    if token:
        provided = request.headers.get("X-Admin-Init-Token") or request.args.get("token")
        if provided != token:
            return error_response(403, "Forbidden")

    migrated_users = 0
    migrated_students = 0
    try:
        with engine.begin() as conn:
            # Users
            upper_users_exists = conn.execute(
                text("SELECT to_regclass('\"Users\"') is not null")
            ).scalar()
            if upper_users_exists:
                rows = conn.execute(
                    text('SELECT username, password_hash, role, full_name, approved, teacher_band, created_at FROM "Users"')
                ).fetchall()
                for r in rows:
                    res = conn.execute(
                        text(
                            "INSERT INTO users (username, password_hash, role, full_name, approved, teacher_band, created_at) "
                            "VALUES (:u,:p,:r,:f,:a,:b,:c) ON CONFLICT (username) DO NOTHING"
                        ),
                        {"u": r[0], "p": r[1], "r": r[2], "f": r[3], "a": r[4], "b": r[5], "c": r[6]},
                    )
                    migrated_users += res.rowcount or 0

            # Students
            upper_students_exists = conn.execute(
                text("SELECT to_regclass('\"Students\"') is not null")
            ).scalar()
            if upper_students_exists:
                rows = conn.execute(
                    text('SELECT student_number, first_name, middle_name, last_name, date_of_birth, grade_level, homeroom_teacher, created_at FROM "Students"')
                ).fetchall()
                for r in rows:
                    res = conn.execute(
                        text(
                            "INSERT INTO students (student_number, first_name, middle_name, last_name, date_of_birth, grade_level, homeroom_teacher, created_at) "
                            "VALUES (:sn,:fn,:mn,:ln,:dob,:gl,:hr,:ca) ON CONFLICT (student_number) DO NOTHING"
                        ),
                        {
                            "sn": r[0],
                            "fn": r[1],
                            "mn": r[2],
                            "ln": r[3],
                            "dob": r[4],
                            "gl": r[5],
                            "hr": r[6],
                            "ca": r[7],
                        },
                    )
                    migrated_students += res.rowcount or 0

        return jsonify({"message": "Force migration complete", "users_migrated": migrated_users, "students_migrated": migrated_students})
    except Exception as exc:
        return error_response(500, "Force migration failed", str(exc))


@app.route("/api/admin/system-repair", methods=["GET"])
def admin_system_repair():
    token = os.environ.get("ADMIN_INIT_TOKEN")
    if token:
        provided = request.headers.get("X-Admin-Init-Token") or request.args.get("token")
        if provided != token:
            return error_response(403, "Forbidden")

    diag = {"db_type": engine.dialect.name}

    ddl_statements = [
        # users table
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='teacher_band') THEN
                ALTER TABLE users ADD COLUMN teacher_band VARCHAR(50);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='approved') THEN
                ALTER TABLE users ADD COLUMN approved INT NOT NULL DEFAULT 1;
            END IF;
        END $$;
        """,
        # grades table
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='grades' AND column_name='raw_score') THEN
                ALTER TABLE grades ADD COLUMN raw_score INT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='grades' AND column_name='max_score') THEN
                ALTER TABLE grades ADD COLUMN max_score INT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='grades' AND column_name='component') THEN
                ALTER TABLE grades ADD COLUMN component VARCHAR(10);
            END IF;
        END $$;
        """,
        # subjects table
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='subjects' AND column_name='weight_ww') THEN
                ALTER TABLE subjects ADD COLUMN weight_ww FLOAT DEFAULT 0;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='subjects' AND column_name='weight_pt') THEN
                ALTER TABLE subjects ADD COLUMN weight_pt FLOAT DEFAULT 0;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='subjects' AND column_name='weight_qa') THEN
                ALTER TABLE subjects ADD COLUMN weight_qa FLOAT DEFAULT 0;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='subjects' AND column_name='teacher_id') THEN
                ALTER TABLE subjects ADD COLUMN teacher_id INTEGER REFERENCES users(id) ON DELETE SET NULL;
            END IF;
        END $$;
        """,
        # sections table
        """
        CREATE TABLE IF NOT EXISTS sections (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            adviser_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
            level_band VARCHAR(10),
            grade_level VARCHAR(10),
            track VARCHAR(50),
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        );
        """,
        # students.section_id
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='students' AND column_name='section_id') THEN
                ALTER TABLE students ADD COLUMN section_id INTEGER REFERENCES sections(id) ON DELETE SET NULL;
            END IF;
        END $$;
        """,
        # student_subjects table
        """
        CREATE TABLE IF NOT EXISTS student_subjects (
            id SERIAL PRIMARY KEY,
            student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
            subject_id INTEGER NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
            teacher_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
            section_id INTEGER REFERENCES sections(id) ON DELETE SET NULL,
            term VARCHAR(20),
            active INT DEFAULT 1,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        );
        """,
        # attendance columns
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='attendance' AND column_name='section_id') THEN
                ALTER TABLE attendance ADD COLUMN section_id INTEGER REFERENCES sections(id) ON DELETE SET NULL;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='attendance' AND column_name='subject_id') THEN
                ALTER TABLE attendance ADD COLUMN subject_id INTEGER REFERENCES subjects(id) ON DELETE SET NULL;
            END IF;
        END $$;
        """,
    ]

    # sqlite alternative for compatibility
    if engine.dialect.name == "sqlite":
        ddl_statements = [
            "ALTER TABLE users ADD COLUMN teacher_band VARCHAR(50);",
            "ALTER TABLE users ADD COLUMN approved INT DEFAULT 1;",
            "ALTER TABLE grades ADD COLUMN raw_score INT;",
            "ALTER TABLE grades ADD COLUMN max_score INT;",
            "ALTER TABLE grades ADD COLUMN component VARCHAR(10);",
            "ALTER TABLE subjects ADD COLUMN weight_ww FLOAT DEFAULT 0;",
            "ALTER TABLE subjects ADD COLUMN weight_pt FLOAT DEFAULT 0;",
            "ALTER TABLE subjects ADD COLUMN weight_qa FLOAT DEFAULT 0;",
            "ALTER TABLE subjects ADD COLUMN teacher_id INTEGER;",
            "CREATE TABLE IF NOT EXISTS sections (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(100) NOT NULL, adviser_id INTEGER, level_band VARCHAR(10), grade_level VARCHAR(10), track VARCHAR(50), created_at DATETIME DEFAULT CURRENT_TIMESTAMP);",
            "ALTER TABLE students ADD COLUMN section_id INTEGER;",
            "CREATE TABLE IF NOT EXISTS student_subjects (id INTEGER PRIMARY KEY AUTOINCREMENT, student_id INTEGER NOT NULL, subject_id INTEGER NOT NULL, teacher_id INTEGER, section_id INTEGER, term VARCHAR(20), active INT DEFAULT 1, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);",
            "ALTER TABLE attendance ADD COLUMN section_id INTEGER;",
            "ALTER TABLE attendance ADD COLUMN subject_id INTEGER;",
        ]

    # Run DDL
    try:
        with engine.begin() as conn:
            for stmt in ddl_statements:
                try:
                    conn.execute(text(stmt))
                except Exception:
                    # ignore if already exists
                    pass
        diag["schema_status"] = "Patched"
    except Exception as exc:
        return error_response(500, "Schema patch failed", str(exc))

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none

    # Admin seed
    try:
        admin_exists = session.query(User).count() > 0
        if not admin_exists:
            session.add(
                User(
                    username="Gabriel_Pena",
                    password_hash="chin1979",
                    role="Admin",
                    full_name="Gabriel Pena",
                    approved=1,
                )
            )
            diag["admin_status"] = "Created"
        else:
            diag["admin_status"] = "Exists"
    except Exception as exc:
        session.rollback()
        session.close()
        return error_response(500, "Admin seed failed", str(exc))

    # Subjects seed
    try:
        subj_count = session.query(Subject).count()
        if subj_count == 0:
            def add_subjects(names, band, category, ww, pt, qa, gmin=None, gmax=None):
                for n in names:
                    session.add(
                        Subject(
                            name=n,
                            category=category,
                            level_band=band,
                            grade_min=gmin,
                            grade_max=gmax,
                            weight_ww=ww,
                            weight_pt=pt,
                            weight_qa=qa,
                        )
                    )

            # JHS groups
            add_subjects(
                ["Filipino 7", "English 7", "Araling Panlipunan 7", "Edukasyon sa Pagpapakatao 7"],
                "JHS",
                "Core",
                0.30,
                0.50,
                0.20,
                7,
                10,
            )
            add_subjects(
                ["Mathematics 7", "Science 7"],
                "JHS",
                "Core",
                0.40,
                0.40,
                0.20,
                7,
                9,
            )
            add_subjects(
                ["Mathematics 10", "Science 10"],
                "JHS",
                "Core",
                0.40,
                0.40,
                0.20,
                10,
                10,
            )
            add_subjects(
                ["MAPEH 7", "TLE 7"],
                "JHS",
                "Core",
                0.20,
                0.60,
                0.20,
                7,
                10,
            )
            # SHS Core
            add_subjects(
                [
                    "Oral Communication",
                    "Reading and Writing",
                    "Komunikasyon at Pananaliksik",
                    "General Mathematics",
                    "Statistics and Probability",
                    "Earth and Life Science",
                    "Physical Education and Health",
                    "Understanding Culture, Society, and Politics",
                ],
                "SHS",
                "Core",
                0.25,
                0.50,
                0.25,
                11,
                12,
            )
            # SHS Applied
            add_subjects(
                [
                    "Empowerment Technologies",
                    "Entrepreneurship",
                    "Practical Research 1",
                    "Practical Research 2",
                    "Inquiries, Investigations, and Immersion",
                ],
                "SHS",
                "Applied",
                0.25,
                0.45,
                0.30,
                11,
                12,
            )
            subj_count = session.query(Subject).count()
        diag["subject_count"] = subj_count
    except Exception as exc:
        session.rollback()
        session.close()
        return error_response(500, "Subject seed failed", str(exc))

    # Students + demo grades seed (to avoid empty UI)
    try:
        student_count = session.query(Student).count()
        if student_count == 0:
            demo_students = [
                {"student_number": "S1001", "first_name": "Juan", "last_name": "Dela Cruz", "grade_level": "Grade 7", "homeroom_teacher": "Gabriel Pena"},
                {"student_number": "S1002", "first_name": "Maria", "last_name": "Santos", "grade_level": "Grade 10", "homeroom_teacher": "Gabriel Pena"},
                {"student_number": "S2001", "first_name": "Ariel", "last_name": "Reyes", "grade_level": "Grade 11", "homeroom_teacher": "Gabriel Pena"},
                {"student_number": "S2002", "first_name": "Bianca", "last_name": "Lim", "grade_level": "Grade 12", "homeroom_teacher": "Gabriel Pena"},
                {"student_number": "S2003", "first_name": "Carlo", "last_name": "Tan", "grade_level": "Grade 12", "homeroom_teacher": "Gabriel Pena"},
            ]
            for s in demo_students:
                session.add(
                    Student(
                        student_number=s["student_number"],
                        first_name=s["first_name"],
                        middle_name=None,
                        last_name=s["last_name"],
                        date_of_birth=None,
                        grade_level=s["grade_level"],
                        homeroom_teacher=s["homeroom_teacher"],
                    )
                )
            session.flush()
            student_count = session.query(Student).count()

        grade_count = session.query(Grade).count()
        if grade_count == 0:
            # Map band to a couple of subjects
            jhs_subjects = session.query(Subject).filter(Subject.level_band == "JHS").limit(2).all()
            shs_subjects = session.query(Subject).filter(Subject.level_band == "SHS").limit(2).all()
            students = session.query(Student).all()
            today = date.today()
            for st in students:
                band = parse_band_from_grade(st.grade_level)
                subj_list = jhs_subjects if band == "JHS" else shs_subjects
                for subj in subj_list:
                    session.add(
                        Grade(
                            student_id=st.id,
                            subject=subj.name,
                            assessment="Activity 1",
                            component="WW",
                            raw_score=40,
                            max_score=50,
                            grade_value=80.0,
                            recorded_on=today,
                            recorded_by=None,
                        )
                    )
            grade_count = session.query(Grade).count()
        diag["student_count"] = student_count
        diag["grade_count"] = grade_count
    except Exception as exc:
        session.rollback()
        session.close()
        return error_response(500, "Demo seed failed", str(exc))

    try:
        session.commit()
    except Exception as exc:
        session.rollback()
        session.close()
        return error_response(500, "Commit failed", str(exc))
    finally:
        session.close()

    return jsonify(diag)


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


class Section(Base):
    __tablename__ = "sections"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    adviser_id = Column(Integer, ForeignKey("users.id"))
    level_band = Column(String(10))  # JHS, SHS
    grade_level = Column(String(10))
    track = Column(String(50))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    adviser = relationship("User")
    students = relationship("Student", back_populates="section")


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
    section_id = Column(Integer, ForeignKey("sections.id"))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    attendance_records = relationship("Attendance", back_populates="student")
    grades = relationship("Grade", back_populates="student")
    behaviors = relationship("BehaviorReport", back_populates="student")
    section = relationship("Section", back_populates="students")


class StudentSubject(Base):
    __tablename__ = "student_subjects"
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    subject_id = Column(Integer, ForeignKey("subjects.id"), nullable=False)
    teacher_id = Column(Integer, ForeignKey("users.id"))
    section_id = Column(Integer, ForeignKey("sections.id"))
    term = Column(String(20))
    active = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class Attendance(Base):
    __tablename__ = "attendance"
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    attendance_date = Column(Date, nullable=False)
    status = Column(String(20), nullable=False)
    recorded_by = Column(Integer, ForeignKey("users.id"))
    section_id = Column(Integer, ForeignKey("sections.id"))
    subject_id = Column(Integer, ForeignKey("subjects.id"))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    student = relationship("Student", back_populates="attendance_records")


class Grade(Base):
    __tablename__ = "grades"
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    subject = Column(String(50), nullable=False)
    assessment = Column(String(100), nullable=False)
    component = Column(String(5))  # WW, PT, QA
    raw_score = Column(Integer)
    max_score = Column(Integer)
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
    recipient = Column(String(100))
    subject = Column(String(150), nullable=False)
    message_body = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    student = relationship("Student")


class Room(Base):
    __tablename__ = "rooms"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    building = Column(String(100))
    level = Column(String(50))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class ScheduleEntry(Base):
    __tablename__ = "schedules"
    id = Column(Integer, primary_key=True, autoincrement=True)
    section_id = Column(Integer, ForeignKey("sections.id"), nullable=False)
    subject_id = Column(Integer, ForeignKey("subjects.id"), nullable=False)
    teacher_id = Column(Integer, ForeignKey("users.id"))
    room_id = Column(Integer, ForeignKey("rooms.id"))
    day_of_week = Column(Integer, nullable=False)  # 0=Mon ... 6=Sun
    start_time = Column(String(5), nullable=False)  # HH:MM
    end_time = Column(String(5), nullable=False)    # HH:MM
    notes = Column(String(200))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class Subject(Base):
    __tablename__ = "subjects"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(150), nullable=False)
    category = Column(String(50), nullable=False)  # Core, Applied, Specialized, Institutional
    level_band = Column(String(10), nullable=False)  # JHS, SHS
    track = Column(String(50))  # e.g., STEM, ABM, HUMSS, ICT, GAS, Institutional
    grade_min = Column(Integer)  # starting grade level (7-12)
    grade_max = Column(Integer)  # ending grade level (7-12)
    weight_ww = Column(Float, nullable=True, server_default="0")
    weight_pt = Column(Float, nullable=True, server_default="0")
    weight_qa = Column(Float, nullable=True, server_default="0")
    teacher_id = Column(Integer, ForeignKey("users.id"))


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
    try:
        with engine.connect() as conn:
            # student_number
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

            # middle_name
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

            # approved
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
                    text(
                        "ALTER TABLE Users ADD approved INT NOT NULL CONSTRAINT DF_Users_Approved DEFAULT 1; "
                        "UPDATE Users SET approved = 1 WHERE approved IS NULL;"
                    )
                )
                conn.commit()

            # teacher_band
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
    if not grade_str:
        return None
    match = re.search(r"(\d+)", str(grade_str))
    if not match:
        return None
    g = int(match.group(1))
    if 7 <= g <= 10:
        return "JHS"
    if 11 <= g <= 12:
        return "SHS"
    return None


def parse_grade_number(grade_str: str):
    """Extract numeric grade level (e.g., 'Grade 9' -> 9)."""
    if not grade_str:
        return None
    m = re.search(r"(\d+)", str(grade_str))
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def auto_assign_subjects_for_student(session, student: "Student", section: "Section" = None):
    """
    Enroll a student into StudentSubject rows based on grade level (and section track if provided).
    """
    grade_num = parse_grade_number(student.grade_level)
    band = parse_band_from_grade(student.grade_level)
    if not band or not grade_num:
        return 0
    track = None
    if section and section.track:
        track = section.track
    # Determine eligible subjects
    subjects = (
        session.query(Subject)
        .filter(Subject.level_band == band)
        .filter(or_(Subject.grade_min == None, Subject.grade_min <= grade_num))  # noqa: E711
        .filter(or_(Subject.grade_max == None, Subject.grade_max >= grade_num))  # noqa: E711
        .all()
    )
    created = 0
    for subj in subjects:
        if subj.track and track and subj.track != track:
            continue
        exists = (
            session.query(StudentSubject.id)
            .filter(
                StudentSubject.student_id == student.id,
                StudentSubject.subject_id == subj.id,
            )
            .first()
        )
        if exists:
            continue
        session.add(
            StudentSubject(
                student_id=student.id,
                subject_id=subj.id,
                teacher_id=subj.teacher_id,
                section_id=section.id if section else student.section_id,
                active=1,
            )
        )
        created += 1
    return created


# Approximate weekly hours per subject for scheduling (DepEd-aligned defaults).
SUBJECT_WEEKLY_HOURS = {
    # JHS cores per grade (typical DepEd time allotment: 300 mins -> 5h; 250 mins -> ~4h)
    "Filipino 7": 4,
    "Filipino 8": 4,
    "Filipino 9": 4,
    "Filipino 10": 4,
    "English 7": 5,
    "English 8": 5,
    "English 9": 5,
    "English 10": 5,
    "Araling Panlipunan 7": 4,
    "Araling Panlipunan 8": 4,
    "Araling Panlipunan 9": 4,
    "Araling Panlipunan 10": 4,
    "Edukasyon sa Pagpapakatao 7": 4,
    "Edukasyon sa Pagpapakatao 8": 4,
    "Edukasyon sa Pagpapakatao 9": 4,
    "Edukasyon sa Pagpapakatao 10": 4,
    "Mathematics 7": 5,
    "Mathematics 8": 5,
    "Mathematics 9": 5,
    "Mathematics 10": 5,
    "Science 7": 5,
    "Science 8": 5,
    "Science 9": 5,
    "Science 10": 5,
    "MAPEH 7": 4,
    "MAPEH 8": 4,
    "MAPEH 9": 4,
    "MAPEH 10": 4,
    "TLE 7": 5,
    "TLE 8": 5,
    "TLE 9": 5,
    "TLE 10": 5,
    # SHS Core (3h default)
    "Oral Communication": 3,
    "Reading and Writing": 3,
    "Komunikasyon at Pananaliksik": 3,
    "General Mathematics": 3,
    "Statistics and Probability": 3,
    "Earth and Life Science": 3,
    "Physical Education and Health": 3,
    "Understanding Culture, Society, and Politics": 3,
    # SHS Applied/Specialized (4h default)
    "Empowerment Technologies": 4,
    "Entrepreneurship": 4,
    "Practical Research 1": 4,
    "Practical Research 2": 4,
    "Inquiries, Investigations, and Immersion": 4,
}


def subject_weekly_hours(subj: "Subject") -> int:
    if subj.name in SUBJECT_WEEKLY_HOURS:
        return SUBJECT_WEEKLY_HOURS[subj.name]
    # fallback by category/band
    if subj.level_band == "JHS":
        return 4
    if subj.level_band == "SHS":
        return 3 if subj.category == "Core" else 4
    return 3


def current_teacher_band():
    role = request.headers.get("X-User-Role")
    if role != "Teacher":
        return None
    band = request.headers.get("X-Teacher-Band")
    if band in ("JHS", "SHS"):
        return band
    return None


def current_teacher_id():
    role = request.headers.get("X-User-Role")
    if role != "Teacher":
        return None
    try:
        return int(request.headers.get("X-User-Id"))
    except (TypeError, ValueError):
        return None


def current_teacher_name():
    role = request.headers.get("X-User-Role")
    if role != "Teacher":
        return None
    return request.headers.get("X-User-Name")


def teacher_advised_section_ids(session, teacher_id: int):
    """Return list of section ids where the teacher is the adviser."""
    if not teacher_id:
        return []
    return [sid for (sid,) in session.query(Section.id).filter(Section.adviser_id == teacher_id).all()]


@app.route("/api/report-card", methods=["GET"])
def report_card():
    student_id = request.args.get("student_id", type=int)
    if not student_id:
        return error_response(400, "student_id required")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        student = session.query(Student).filter_by(id=student_id).first()
        if not student:
            return error_response(404, "Student not found")

        # basic access control: Admin can view all; Teacher only if same band OR homeroom
        role = request.headers.get("X-User-Role")
        band = parse_band_from_grade(student.grade_level)
        if role == "Teacher":
            teacher_band = current_teacher_band()
            teacher_name = current_teacher_name()
            homeroom_ok = False
            if teacher_name and student.homeroom_teacher:
                homeroom_ok = teacher_name.strip().lower() == str(student.homeroom_teacher).strip().lower()
            if teacher_band and band and teacher_band != band and not homeroom_ok:
                return error_response(403, "Forbidden for this student band")

        grades = session.query(Grade).filter(Grade.student_id == student_id).all()
        by_subject = {}
        for g in grades:
            by_subject.setdefault(g.subject, []).append(g)
        subjects_summary = []
        for subj, gs in by_subject.items():
            # average grade_value
            vals = [float(x.grade_value) for x in gs if x.grade_value is not None]
            avg = sum(vals) / len(vals) if vals else 0
            subjects_summary.append(
                {
                    "subject": subj,
                    "average": round(avg, 2),
                    "entries": len(gs),
                }
            )
        return jsonify(
            {
                "student": {
                    "id": student.id,
                    "name": f"{student.first_name} {student.last_name}",
                    "grade_level": student.grade_level,
                    "section_id": student.section_id,
                },
                "subjects": subjects_summary,
            }
        )
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


def time_to_minutes(tstr: str) -> int:
    h, m = tstr.split(":")
    return int(h) * 60 + int(m)


def minutes_to_str(mins: int) -> str:
    h = mins // 60
    m = mins % 60
    return f"{h:02d}:{m:02d}"


def generate_slots(include_saturday: bool = False):
    """
    Generate 1-hour slots from 07:00-12:00 and 13:00-17:00 (lunch 12:00-13:00).
    """
    slots = []
    days = list(range(5 + (1 if include_saturday else 0)))  # 0-4 or 0-5
    hour_ranges = [(7, 12), (13, 17)]
    for day in days:
        for start, end in hour_ranges:
            for h in range(start, end):
                slots.append((day, f"{h:02d}:00", f"{h+1:02d}:00"))
    return slots


def split_hours_into_blocks(hours: int):
    """Split weekly hours into 1-3 blocks to avoid marathon sessions."""
    if hours >= 5:
        return [3, hours - 3]
    if hours == 4:
        return [2, 2]
    if hours == 3:
        return [2, 1]
    if hours == 2:
        return [2]
    return [1]


def subjects_for_section(session, section: "Section"):
    band = section.level_band or parse_band_from_grade(section.grade_level)
    grade_num = parse_grade_number(section.grade_level)
    if not band:
        return []
    q = session.query(Subject).filter(Subject.level_band == band)
    if grade_num:
        q = q.filter(or_(Subject.grade_min == None, Subject.grade_min <= grade_num))  # noqa: E711
        q = q.filter(or_(Subject.grade_max == None, Subject.grade_max >= grade_num))  # noqa: E711
    return q.all()


def ensure_default_room(session):
    room = session.query(Room).first()
    if room:
        return room
    room = Room(name="Room A", building="Main", level="1")
    session.add(room)
    session.commit()
    return room


def has_conflict(entries, day, start, end, key):
    """Check conflict in a dict keyed by day -> list of (start,end, keyVal) for either section/teacher/room."""
    if day not in entries:
        return False
    start_m = time_to_minutes(start)
    end_m = time_to_minutes(end)
    for (s, e, k) in entries[day]:
        if key is not None and k is not None and k != key:
            continue
        if not (end_m <= time_to_minutes(s) or start_m >= time_to_minutes(e)):
            return True
    return False


def record_block(entries, day, start, end, key):
    entries.setdefault(day, []).append((start, end, key))


def format_time_12h_str(t: str) -> str:
    """Convert 'HH:MM' (24h) to 'H:MM AM/PM' for readability."""
    if not t or ":" not in t:
        return t or ""
    try:
        h_str, m_str = t.split(":")
        h = int(h_str)
        m = m_str
        ampm = "PM" if h >= 12 else "AM"
        h = h % 12
        if h == 0:
            h = 12
        return f"{h}:{m} {ampm}"
    except Exception:
        return t


def pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def make_schedule_pdf(entries, title="Schedule"):
    """
    Minimal PDF generator for schedule table. Avoids external deps.
    """
    buf = io.BytesIO()
    lines = []
    lines.append("%PDF-1.4")
    objs = []

    # Prepare content stream
    y_start = 780
    y = y_start
    content = []
    content.append("BT /F1 12 Tf 50 800 Td")
    content.append(f"({pdf_escape(title)}) Tj")
    content.append("ET")
    y -= 24
    headers = ["Day", "Time", "Section", "Subject", "Teacher", "Room"]
    rows = []
    for r in entries:
        rows.append(
            [
                day_name_short(r.get("day_of_week")),
                f"{format_time_12h_str(r.get('start_time',''))} - {format_time_12h_str(r.get('end_time',''))}",
                r.get("section_name", "") or "-",
                r.get("subject_name", "") or "-",
                r.get("teacher_name", "") or "-",
                r.get("room_name", "") or "-",
            ]
        )
    table = [headers] + rows
    col_widths = [80, 100, 120, 140, 140, 100]
    content.append("BT /F1 10 Tf")
    y = y_start - 40
    for i, row in enumerate(table):
        x = 40
        for text, width in zip(row, col_widths):
            content.append(f"1 0 0 1 {x} {y} Tm ({pdf_escape(str(text))}) Tj")
            x += width
        y -= 14
        if y < 40:
            break  # simple single-page guard
    content.append("ET")
    content_bytes = "\n".join(content).encode("utf-8")

    # Objects
    objs.append("1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj")
    objs.append("2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj")
    objs.append(
        "3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj"
    )
    objs.append(f"4 0 obj << /Length {len(content_bytes)} >> stream\n".encode("utf-8"))
    objs[-1] = objs[-1] + content_bytes + b"\nendstream\nendobj"
    objs.append("5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj")

    # Build xref
    offsets = []
    buf.write(lines[0].encode("utf-8") + b"\n")
    for obj in objs:
        offsets.append(buf.tell())
        if isinstance(obj, bytes):
            buf.write(obj + b"\n")
        else:
            buf.write(obj.encode("utf-8") + b"\n")
    xref_pos = buf.tell()
    buf.write(f"xref\n0 {len(objs)+1}\n".encode("utf-8"))
    buf.write(b"0000000000 65535 f \n")
    for off in offsets:
        buf.write(f"{off:010} 00000 n \n".encode("utf-8"))
    buf.write(
        f"trailer << /Size {len(objs)+1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF".encode(
            "utf-8"
        )
    )
    buf.seek(0)
    return buf


def day_name_short(idx: int) -> str:
    names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    if idx is None:
        return "-"
    if 0 <= idx < len(names):
        return names[idx]
    return str(idx)


# Create missing tables (Communications, Subjects) without touching existing ones
try:
    Base.metadata.create_all(
        bind=engine,
        tables=[
            CommunicationMessage.__table__,
            Subject.__table__,
            Section.__table__,
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
        user = session.query(User).filter(User.username == username).first()
        if not user or not verify_password(user.password_hash, password):
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
                "section_id": s.section_id,
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
        existing = session.query(Student).filter_by(student_number=data["student_number"].strip()).first()
        if existing:
            full = f"{existing.first_name} {existing.last_name}".strip()
            return error_response(
                409,
                "student_number must be unique",
                f"Student number already used by {full} (id {existing.id}). Edit the existing record or choose a new number.",
            )
        section_obj = None
        section_id = data.get("section_id")
        if section_id:
            section_obj = session.query(Section).filter_by(id=section_id).first()
            if not section_obj:
                return error_response(400, "section_id not found")

        adviser_name = None
        if section_obj and section_obj.adviser_id:
            adviser = session.query(User).filter_by(id=section_obj.adviser_id).first()
            if adviser:
                adviser_name = adviser.full_name or adviser.username

        student = Student(
            student_number=data["student_number"].strip(),
            first_name=data["first_name"].strip(),
            middle_name=data.get("middle_name", None),
            last_name=data["last_name"].strip(),
            date_of_birth=date.fromisoformat(data["date_of_birth"])
            if data.get("date_of_birth")
            else None,
            grade_level=data.get("grade_level"),
            homeroom_teacher=adviser_name or data.get("homeroom_teacher"),
            section_id=section_obj.id if section_obj else None,
        )
        session.add(student)
        session.flush()
        try:
            auto_assign_subjects_for_student(session, student, section_obj)
        except Exception as exc:
            logging.warning("auto assign subjects failed: %s", exc)
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
        session.query(StudentSubject).filter_by(student_id=student_id).delete(synchronize_session=False)
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

    teacher_id = current_teacher_id()
    if teacher_id:
        subj = SessionLocal().query(Subject).filter(Subject.name == data.get("subject")).first()
        if subj and subj.teacher_id not in (None, teacher_id):
            return error_response(403, "Not allowed to grade this subject")

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
            component=data.get("component"),
            raw_score=data.get("raw_score"),
            max_score=data.get("max_score"),
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


@app.route("/api/grades/bulk", methods=["POST"])
def bulk_save_grades():
    data = request.get_json(silent=True) or []
    if not isinstance(data, list):
        return error_response(400, "Payload must be a list")

    teacher_id = current_teacher_id()
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        count_upsert = 0
        for item in data:
            required = ["student_id", "subject", "assessment", "component", "raw_score", "max_score"]
            missing = [f for f in required if item.get(f) is None]
            if missing:
                session.rollback()
                return error_response(400, f"Missing fields: {', '.join(missing)}")
            if teacher_id:
                subj = session.query(Subject).filter(Subject.name == item.get("subject")).first()
                if subj and subj.teacher_id not in (None, teacher_id):
                    session.rollback()
                    return error_response(403, "Not allowed to grade this subject")
            raw = int(item.get("raw_score", 0))
            maxs = int(item.get("max_score", 0))
            grade_val = float(raw) / maxs * 100 if maxs > 0 else 0.0
            rec_on = item.get("recorded_on")
            try:
                recorded_date = date.fromisoformat(rec_on) if rec_on else date.today()
            except ValueError:
                session.rollback()
                return error_response(400, "recorded_on must be YYYY-MM-DD")
            existing = (
                session.query(Grade)
                .filter(
                    Grade.student_id == item["student_id"],
                    Grade.subject == item["subject"],
                    Grade.assessment == item["assessment"],
                )
                .first()
            )
            if existing:
                existing.component = item.get("component")
                existing.raw_score = raw
                existing.max_score = maxs
                existing.grade_value = grade_val
                existing.recorded_on = recorded_date
                existing.recorded_by = item.get("recorded_by")
            else:
                g = Grade(
                    student_id=item["student_id"],
                    subject=item["subject"],
                    assessment=item["assessment"],
                    component=item.get("component"),
                    raw_score=raw,
                    max_score=maxs,
                    grade_value=grade_val,
                    recorded_on=recorded_date,
                    recorded_by=item.get("recorded_by"),
                )
                session.add(g)
            count_upsert += 1
        session.commit()
        return jsonify({"message": "Bulk grades saved", "count": count_upsert})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/grades", methods=["GET"])
def list_grades():
    student_id = request.args.get("student_id", type=int)
    subject = request.args.get("subject")
    section_id = request.args.get("section_id", type=int)
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        band = current_teacher_band()
        teacher_id = current_teacher_id()
        query = session.query(Grade)
        if student_id:
            query = query.filter(Grade.student_id == student_id)
        if subject:
            query = query.filter(Grade.subject == subject)
        if section_id:
            query = query.join(Student, Student.id == Grade.student_id).filter(Student.section_id == section_id)
        if teacher_id:
            query = query.join(Subject, Subject.name == Grade.subject).filter(
                or_(Subject.teacher_id == None, Subject.teacher_id == teacher_id)  # noqa: E711
            )
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
                    "component": g.component,
                    "raw_score": g.raw_score,
                    "max_score": g.max_score,
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
        teacher_id = current_teacher_id()
        if teacher_id:
            subj = session.query(Subject).filter(Subject.name == grade.subject).first()
            if subj and subj.teacher_id not in (None, teacher_id):
                return error_response(403, "Not allowed to modify this subject")
        for field in ["subject", "assessment", "grade_value", "recorded_by", "component", "raw_score", "max_score"]:
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
        teacher_id = current_teacher_id()
        if teacher_id:
            subj = session.query(Subject).filter(Subject.name == grade.subject).first()
            if subj and subj.teacher_id not in (None, teacher_id):
                return error_response(403, "Not allowed to delete this subject")
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
    if len(data.get("password", "")) < 8:
        return error_response(400, "Password must be at least 8 characters")

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
            password_hash=hash_password(data["password"]),
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
            if len(data["password"]) < 8:
                return error_response(400, "Password must be at least 8 characters")
            user.password_hash = hash_password(data["password"])
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
        # Clear foreign-key references before deletion to avoid FK violations
        session.query(Grade).filter(Grade.recorded_by == user_id).update(
            {Grade.recorded_by: None}, synchronize_session=False
        )
        session.query(Attendance).filter(Attendance.recorded_by == user_id).update(
            {Attendance.recorded_by: None}, synchronize_session=False
        )
        session.query(BehaviorReport).filter(BehaviorReport.reported_by == user_id).update(
            {BehaviorReport.reported_by: None}, synchronize_session=False
        )
        session.query(Subject).filter(Subject.teacher_id == user_id).update(
            {Subject.teacher_id: None}, synchronize_session=False
        )
        session.query(Section).filter(Section.adviser_id == user_id).update(
            {Section.adviser_id: None}, synchronize_session=False
        )
        session.query(ScheduleEntry).filter(ScheduleEntry.teacher_id == user_id).update(
            {ScheduleEntry.teacher_id: None}, synchronize_session=False
        )
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


@app.route("/api/sections", methods=["GET"])
def list_sections():
    level_band = request.args.get("level_band")
    adviser_id = request.args.get("adviser_id", type=int)
    grade_level = request.args.get("grade_level")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        band_header = current_teacher_band()
        query = session.query(Section)
        if level_band:
            query = query.filter(Section.level_band == level_band)
        if band_header:
            query = query.filter(Section.level_band == band_header)
        if adviser_id:
            query = query.filter(Section.adviser_id == adviser_id)
        if grade_level:
            query = query.filter(Section.grade_level == grade_level)
        sections = query.order_by(Section.name.asc()).all()
        result = []
        for s in sections:
            count = session.query(func.count(Student.id)).filter(Student.section_id == s.id).scalar() or 0
            adviser_name = None
            if s.adviser_id:
                adv = session.query(User).filter_by(id=s.adviser_id).first()
                adviser_name = adv.full_name if adv else None
            result.append(
                {
                    "id": s.id,
                    "name": s.name,
                    "adviser_id": s.adviser_id,
                    "adviser_name": adviser_name,
                    "level_band": s.level_band,
                    "grade_level": s.grade_level,
                    "track": s.track,
                    "student_count": count,
                }
            )
        return jsonify(result)
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/sections", methods=["POST"])
def create_section():
    admin_err = require_admin()
    if admin_err:
        return admin_err
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return error_response(400, "name is required")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        adviser_id = data.get("adviser_id")
        if adviser_id:
            exists = session.query(User.id).filter_by(id=adviser_id).first()
            if not exists:
                return error_response(400, "adviser_id not found")
        section = Section(
            name=name,
            adviser_id=adviser_id,
            level_band=data.get("level_band"),
            grade_level=data.get("grade_level"),
            track=data.get("track"),
        )
        session.add(section)
        session.commit()
        return jsonify({"message": "Section created", "id": section.id}), 201
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/sections/<int:section_id>", methods=["PUT"])
def update_section(section_id: int):
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
        section = session.query(Section).filter_by(id=section_id).first()
        if not section:
            return error_response(404, "Section not found")
        if "name" in data and data["name"]:
            section.name = data["name"].strip()
        if "adviser_id" in data:
            adv_id = data["adviser_id"]
            if adv_id:
                exists = session.query(User.id).filter_by(id=adv_id).first()
                if not exists:
                    return error_response(400, "adviser_id not found")
            section.adviser_id = adv_id
        for fld in ("level_band", "grade_level", "track"):
            if fld in data:
                setattr(section, fld, data[fld])
        session.commit()
        return jsonify({"message": "Section updated"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/sections/<int:section_id>", methods=["DELETE"])
def delete_section(section_id: int):
    admin_err = require_admin()
    if admin_err:
        return admin_err
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        section = session.query(Section).filter_by(id=section_id).first()
        if not section:
            return error_response(404, "Section not found")
        session.query(Student).filter(Student.section_id == section_id).update(
            {Student.section_id: None}, synchronize_session=False
        )
        session.delete(section)
        session.commit()
        return jsonify({"message": "Section deleted"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/sections/<int:section_id>/students", methods=["POST"])
def assign_students_to_section(section_id: int):
    admin_err = require_admin()
    if admin_err:
        return admin_err
    data = request.get_json(silent=True) or {}
    ids = data.get("student_ids") or []
    if not isinstance(ids, list) or not ids:
        return error_response(400, "student_ids list is required")

    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        section = session.query(Section).filter_by(id=section_id).first()
        if not section:
            return error_response(404, "Section not found")
        sec_grade_num = parse_grade_number(section.grade_level)
        valid_ids = []
        for sid in ids:
            st = session.query(Student).filter(Student.id == sid).first()
            if not st:
                session.rollback()
                return error_response(404, f"Student {sid} not found")
            stu_grade_num = parse_grade_number(st.grade_level)
            if sec_grade_num and stu_grade_num and stu_grade_num != sec_grade_num:
                session.rollback()
                return error_response(400, f"Student grade {stu_grade_num} does not match section grade {sec_grade_num}")
            valid_ids.append(sid)
        if valid_ids:
            update_payload = {Student.section_id: section_id}
            if section.adviser_id:
                adviser = session.query(User).filter_by(id=section.adviser_id).first()
                if adviser:
                    update_payload[Student.homeroom_teacher] = adviser.full_name or adviser.username
            session.query(Student).filter(Student.id.in_(valid_ids)).update(
                update_payload, synchronize_session=False
            )
            session.flush()
            try:
                for sid in valid_ids:
                    stu = session.query(Student).filter(Student.id == sid).first()
                    if stu:
                        auto_assign_subjects_for_student(session, stu, section)
            except Exception as exc:
                logging.warning("auto assign subjects on section assign failed: %s", exc)
        session.commit()
        return jsonify({"message": "Students assigned", "section_id": section_id, "count": len(ids)})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/rooms", methods=["GET", "POST"])
def rooms():
    if request.method == "GET":
        session_or_none = get_session()
        if isinstance(session_or_none, tuple):
            session, exc = session_or_none
            return error_response(500, "Database connection failed", str(exc))
        session = session_or_none
        try:
            rooms = session.query(Room).order_by(Room.name.asc()).all()
            return jsonify(
                [
                    {"id": r.id, "name": r.name, "building": r.building, "level": r.level}
                    for r in rooms
                ]
            )
        except Exception as exc:
            return error_response(500, "Unexpected error", str(exc))
        finally:
            session.close()
    # POST create room (admin only)
    admin_err = require_admin()
    if admin_err:
        return admin_err
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return error_response(400, "name is required")
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        exists = session.query(Room).filter_by(name=name).first()
        if exists:
            return error_response(409, "room name must be unique")
        room = Room(name=name, building=data.get("building"), level=data.get("level"))
        session.add(room)
        session.commit()
        return jsonify({"message": "Room created", "id": room.id}), 201
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/rooms/<int:room_id>", methods=["DELETE"])
def delete_room(room_id: int):
    admin_err = require_admin()
    if admin_err:
        return admin_err
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        room = session.query(Room).filter_by(id=room_id).first()
        if not room:
            return error_response(404, "Room not found")
        session.query(ScheduleEntry).filter(ScheduleEntry.room_id == room_id).delete(
            synchronize_session=False
        )
        session.delete(room)
        session.commit()
        return jsonify({"message": "Room deleted"})
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/schedule", methods=["GET"])
def list_schedule():
    section_id = request.args.get("section_id", type=int)
    teacher_id = request.args.get("teacher_id", type=int)
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        q = session.query(ScheduleEntry)
        if section_id:
            q = q.filter(ScheduleEntry.section_id == section_id)
        if teacher_id:
            q = q.filter(ScheduleEntry.teacher_id == teacher_id)
        rows = q.all()
        result = []
        for r in rows:
            subj = session.query(Subject).filter_by(id=r.subject_id).first()
            sec = session.query(Section).filter_by(id=r.section_id).first()
            teacher = session.query(User).filter_by(id=r.teacher_id).first() if r.teacher_id else None
            room = session.query(Room).filter_by(id=r.room_id).first() if r.room_id else None
            result.append(
                {
                    "id": r.id,
                    "section_id": r.section_id,
                    "section_name": sec.name if sec else None,
                    "subject_id": r.subject_id,
                    "subject_name": subj.name if subj else None,
                    "teacher_id": r.teacher_id,
                    "teacher_name": teacher.full_name if teacher else None,
                    "room_id": r.room_id,
                    "room_name": room.name if room else None,
                    "day_of_week": r.day_of_week,
                    "start_time": r.start_time,
                    "end_time": r.end_time,
                    "notes": r.notes,
                }
            )
        return jsonify(result)
    except Exception as exc:
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/schedule/auto-generate", methods=["POST"])
def auto_generate_schedule():
    admin_err = require_admin()
    if admin_err:
        return admin_err
    data = request.get_json(silent=True) or {}
    section_id = data.get("section_id")
    allow_saturday = bool(data.get("allow_saturday", False))
    if not section_id:
        return error_response(400, "section_id is required")
    # Make sure subjects exist (fresh DB safety)
    ensure_subjects_catalog()
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        section = session.query(Section).filter_by(id=section_id).first()
        if not section:
            return error_response(404, "Section not found")
        ensure_default_room(session)
        rooms = session.query(Room).order_by(Room.name.asc()).all()

        # Preload existing schedules to avoid conflicts across sections
        existing = session.query(ScheduleEntry).all()
        section_occ = {}
        teacher_occ = {}
        room_occ = {}
        for r in existing:
            if r.section_id == section_id:
                continue  # will replace
            record_block(section_occ, r.day_of_week, r.start_time, r.end_time, r.section_id)
            if r.teacher_id:
                record_block(teacher_occ, r.day_of_week, r.start_time, r.end_time, r.teacher_id)
            if r.room_id:
                record_block(room_occ, r.day_of_week, r.start_time, r.end_time, r.room_id)

        # Clear existing schedule for section
        session.query(ScheduleEntry).filter(ScheduleEntry.section_id == section_id).delete(
            synchronize_session=False
        )

        subjects = subjects_for_section(session, section)
        if not subjects:
            return error_response(400, f"No subjects available for section grade {section.grade_level}")
        slots = generate_slots(include_saturday=allow_saturday)
        # Organize slots by day for easier contiguous search
        slots_by_day = {}
        for day, start, end in slots:
            slots_by_day.setdefault(day, []).append((start, end))

        created = []
        failures = []
        for subj in subjects:
            hours = subject_weekly_hours(subj)
            blocks = split_hours_into_blocks(hours)
            teacher_id = subj.teacher_id
            for blk in blocks:
                assigned = False
                for day, day_slots in slots_by_day.items():
                    if len(day_slots) < blk:
                        continue
                    for idx in range(0, len(day_slots) - blk + 1):
                        start = day_slots[idx][0]
                        end = day_slots[idx + blk - 1][1]
                        # check conflicts for section and teacher and rooms
                        if has_conflict(section_occ, day, start, end, section.id):
                            continue
                        if teacher_id and has_conflict(teacher_occ, day, start, end, teacher_id):
                            continue
                        room_choice = None
                        for room in rooms:
                            if has_conflict(room_occ, day, start, end, room.id):
                                continue
                            room_choice = room
                            break
                        if not room_choice:
                            continue
                        # assign
                        entry = ScheduleEntry(
                            section_id=section.id,
                            subject_id=subj.id,
                            teacher_id=teacher_id,
                            room_id=room_choice.id,
                            day_of_week=day,
                            start_time=start,
                            end_time=end,
                            notes=None,
                        )
                        session.add(entry)
                        record_block(section_occ, day, start, end, section.id)
                        if teacher_id:
                            record_block(teacher_occ, day, start, end, teacher_id)
                        if room_choice:
                            record_block(room_occ, day, start, end, room_choice.id)
                        created.append(
                            {
                                "subject": subj.name,
                                "day": day,
                                "start": start,
                                "end": end,
                                "room": room_choice.name,
                            }
                        )
                        assigned = True
                        break
                    if assigned:
                        break
                if not assigned:
                    failures.append({"subject": subj.name, "hours": blk})

        session.commit()
        return jsonify({"created": created, "failed": failures})
    except Exception as exc:
        session.rollback()
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
        teacher_id = current_teacher_id()
        query = session.query(Subject)
        if level_band:
            query = query.filter(Subject.level_band == level_band)
        if band_header:
            query = query.filter(Subject.level_band == band_header)
        if teacher_id:
            query = query.filter(
                or_(
                    Subject.teacher_id == None,  # noqa: E711 allow unassigned
                    Subject.teacher_id == teacher_id,
                )
            )
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
                    "weight_ww": s.weight_ww,
                    "weight_pt": s.weight_pt,
                    "weight_qa": s.weight_qa,
                    "teacher_id": s.teacher_id,
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
        teacher_id = data.get("teacher_id")
        if teacher_id:
            exists = session.query(User.id).filter(User.id == teacher_id).first()
            if not exists:
                return error_response(400, "teacher_id not found")
        subject = Subject(
            name=data["name"].strip(),
            category=category,
            level_band=level_band,
            track=data.get("track"),
            grade_min=grade_min,
            grade_max=grade_max,
            teacher_id=teacher_id,
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
        if "teacher_id" in data:
            tid = data["teacher_id"]
            if tid:
                exists = session.query(User.id).filter(User.id == tid).first()
                if not exists:
                    return error_response(400, "teacher_id not found")
            subject.teacher_id = tid
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
    section_id = request.args.get("section_id", type=int)
    subject_id = request.args.get("subject_id", type=int)
    att_date = request.args.get("attendance_date")
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        band = current_teacher_band()
        teacher_id = current_teacher_id()
        advised_sections = teacher_advised_section_ids(session, teacher_id) if teacher_id else []

        query = session.query(Attendance)
        if student_id:
            query = query.filter(Attendance.student_id == student_id)
        if section_id:
            query = query.filter(Attendance.section_id == section_id)
        if subject_id:
            query = query.filter(Attendance.subject_id == subject_id)
        if att_date:
            try:
                parsed = date.fromisoformat(att_date)
                query = query.filter(Attendance.attendance_date == parsed)
            except ValueError:
                return error_response(400, "attendance_date must be YYYY-MM-DD")

        if teacher_id:
            allowed_subject_ids = [
                sid for (sid,) in session.query(Subject.id).filter(or_(Subject.teacher_id == None, Subject.teacher_id == teacher_id)).all()  # noqa: E711
            ]
            if subject_id and subject_id not in allowed_subject_ids and section_id not in advised_sections:
                return error_response(403, "Forbidden for this subject/section")
            query = query.filter(
                or_(
                    Attendance.subject_id.in_(allowed_subject_ids),
                    Attendance.section_id.in_(advised_sections),
                )
            )

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
                    "section_id": a.section_id,
                    "subject_id": a.subject_id,
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
        teacher_id = current_teacher_id()
        subject_id = data.get("subject_id")
        section_id = data.get("section_id") or student.section_id
        if subject_id:
            subj = session.query(Subject).filter_by(id=subject_id).first()
            if not subj:
                return error_response(400, "subject_id not found")
            if teacher_id and subj.teacher_id not in (None, teacher_id):
                return error_response(403, "Not allowed to record for this subject")
        record = Attendance(
            student_id=data["student_id"],
            attendance_date=attendance_date,
            status=data["status"],
            recorded_by=data.get("recorded_by"),
            section_id=section_id,
            subject_id=subject_id,
        )
        session.add(record)
        session.commit()
        return jsonify({"message": "Attendance recorded", "id": record.id}), 201
    except Exception as exc:
        session.rollback()
        return error_response(500, "Unexpected error", str(exc))
    finally:
        session.close()


@app.route("/api/attendance/bulk", methods=["POST"])
def bulk_attendance():
    data = request.get_json(silent=True) or {}
    attendance_date = data.get("attendance_date")
    records = data.get("records") or []
    section_id = data.get("section_id")
    subject_id = data.get("subject_id")
    if not attendance_date:
        return error_response(400, "attendance_date required")
    try:
        att_date = date.fromisoformat(attendance_date)
    except ValueError:
        return error_response(400, "attendance_date must be YYYY-MM-DD")
    if not isinstance(records, list) or not records:
        return error_response(400, "records list is required")

    teacher_id = current_teacher_id()
    session_or_none = get_session()
    if isinstance(session_or_none, tuple):
        session, exc = session_or_none
        return error_response(500, "Database connection failed", str(exc))
    session = session_or_none
    try:
        subj_obj = None
        if subject_id:
            subj_obj = session.query(Subject).filter_by(id=subject_id).first()
            if not subj_obj:
                return error_response(400, "subject_id not found")
            if teacher_id and subj_obj.teacher_id not in (None, teacher_id):
                return error_response(403, "Not allowed to record for this subject")
        sec_obj = None
        if section_id:
            sec_obj = session.query(Section).filter_by(id=section_id).first()
            if not sec_obj:
                return error_response(400, "section_id not found")
            if teacher_id and sec_obj.adviser_id not in (None, teacher_id) and (not subj_obj or subj_obj.teacher_id not in (None, teacher_id)):
                return error_response(403, "Not allowed to record for this section")

        saved = 0
        for rec in records:
            sid = rec.get("student_id")
            status = rec.get("status") or "Present"
            if not sid:
                continue
            student = session.query(Student).filter_by(id=sid).first()
            if not student:
                continue
            if sec_obj and student.section_id != sec_obj.id:
                # keep scoped to the section sheet
                continue
            if teacher_id and not sec_obj and subj_obj and subj_obj.teacher_id not in (None, teacher_id):
                continue
            target_section = sec_obj.id if sec_obj else student.section_id
            existing = (
                session.query(Attendance)
                .filter(
                    Attendance.student_id == sid,
                    Attendance.attendance_date == att_date,
                    Attendance.section_id == target_section,
                    Attendance.subject_id == (subj_obj.id if subj_obj else None),
                )
                .first()
            )
            if existing:
                existing.status = status
                existing.recorded_by = rec.get("recorded_by")
                saved += 1
            else:
                session.add(
                    Attendance(
                        student_id=sid,
                        attendance_date=att_date,
                        status=status,
                        recorded_by=rec.get("recorded_by"),
                        section_id=target_section,
                        subject_id=subj_obj.id if subj_obj else None,
                    )
                )
                saved += 1
        session.commit()
        return jsonify({"message": "Attendance sheet saved", "count": saved})
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
        for field in ["status", "recorded_by", "student_id", "section_id", "subject_id"]:
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

