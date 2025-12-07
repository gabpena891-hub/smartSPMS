from app import app, engine, Base, Subject, SessionLocal


def seed_data():
    session = SessionLocal()
    try:
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

        # Group A: JHS Languages, AP, EsP
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

        # Group B: JHS Math & Science
        add_subjects(
            ["Mathematics 7", "Science 7", "Mathematics 10", "Science 10"],
            "JHS",
            "Core",
            0.40,
            0.40,
            0.20,
            7,
            10,
        )

        # Group C: JHS MAPEH/TLE
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

        # Group D: SHS Core Subjects
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

        # Group E: SHS Applied/Specialized
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

        session.commit()
        print("Subjects seeded successfully.")
    except Exception as exc:
        session.rollback()
        print(f"Seeding failed: {exc}")
    finally:
        session.close()


if __name__ == "__main__":
    with app.app_context():
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)
        seed_data()
        print("Database refreshed and seeded!")

