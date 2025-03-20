from sqlalchemy import func, desc
from entity.models import Student, Grade, Subject, Group
from conf.db import SessionLocal

def select_1():
    """Find the top 5 students with the highest average grade across all subjects."""
    with SessionLocal() as session:
        result = session.query(Student.name, func.avg(Grade.grade).label("avg_grade")) \
            .join(Grade) \
            .group_by(Student.id) \
            .order_by(desc("avg_grade")) \
            .limit(5).all()
    print("Top 5 students with the highest average grade:", result)
    return result

def select_2(subject_id: int):
    """Find the student with the highest average grade in a specific subject."""
    with SessionLocal() as session:
        result = session.query(Student.name, func.avg(Grade.grade).label("avg_grade")) \
            .join(Grade) \
            .filter(Grade.subject_id == subject_id) \
            .group_by(Student.id) \
            .order_by(desc("avg_grade")) \
            .first()
    print(f"Student with the highest average grade in subject {subject_id}:", result)
    return result

def select_3(subject_id: int):
    """Find the average grade in groups for a specific subject."""
    with SessionLocal() as session:
        result = session.query(Group.name, func.avg(Grade.grade).label("avg_grade")) \
            .join(Student).join(Grade) \
            .filter(Grade.subject_id == subject_id) \
            .group_by(Group.id).all()
    print(f"Average grade in groups for subject {subject_id}:", result)
    return result

def select_4():
    """Find the average grade across all grades."""
    with SessionLocal() as session:
        result = session.query(func.avg(Grade.grade)).scalar()
    print("Average grade across all grades:", result)
    return result

def select_5(teacher_id: int):
    """Find the courses taught by a specific teacher."""
    with SessionLocal() as session:
        result = session.query(Subject.name).filter(Subject.teacher_id == teacher_id).all()
    print(f"Courses taught by teacher {teacher_id}:", result)
    return result

def select_6(group_id: int):
    """Find the list of students in a specific group."""
    with SessionLocal() as session:
        result = session.query(Student.name, Student.last_name) \
            .filter(Student.group_id == group_id).all()
    print(f"List of students in group {group_id}:", result)
    return result

def select_7(group_id: int, subject_id: int):
    """Find the grades of students in a specific group for a specific subject."""
    with SessionLocal() as session:
        result = session.query(Student.name, Grade.grade, Grade.date_received) \
            .join(Grade) \
            .filter(Student.group_id == group_id, Grade.subject_id == subject_id).all()
    print(f"Grades of students in group {group_id} for subject {subject_id}:", result)
    return result

def select_8(teacher_id: int):
    """Find the average grade given by a specific teacher across their subjects."""
    with SessionLocal() as session:
        result = session.query(func.avg(Grade.grade)) \
            .join(Subject) \
            .filter(Subject.teacher_id == teacher_id).scalar()
    print(f"Average grade given by teacher {teacher_id}:", result)
    return result

def select_9(student_id: int):
    """Find the list of courses attended by a specific student."""
    with SessionLocal() as session:
        result = session.query(Subject.name).join(Grade) \
            .filter(Grade.student_id == student_id).distinct().all()
    print(f"Courses attended by student {student_id}:", result)
    return result

def select_10(student_id: int, teacher_id: int):
    """Find the list of courses taught by a specific teacher to a specific student."""
    with SessionLocal() as session:
        result = session.query(Subject.name).join(Grade) \
            .filter(Grade.student_id == student_id, Subject.teacher_id == teacher_id).distinct().all()
    print(f"Courses taught by teacher {teacher_id} to student {student_id}:", result)
    return result

# Execute functions and print results to the console
if __name__ == "__main__":
    print("\n--- Executing SQL queries ---\n")

    select_1()
    select_2(subject_id=1)
    select_3(subject_id=1)
    select_4()
    select_5(teacher_id=1)
    select_6(group_id=1)
    select_7(group_id=1, subject_id=1)
    select_8(teacher_id=1)
    select_9(student_id=1)
    select_10(student_id=1, teacher_id=1)

    print("\n--- Execution completed ---")