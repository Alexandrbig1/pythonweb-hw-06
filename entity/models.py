from sqlalchemy import ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from datetime import datetime


class Base(DeclarativeBase):
    pass


class Group(Base):
    __tablename__ = 'groups'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    students: Mapped[list['Student']] = relationship(back_populates='group')


class Student(Base):
    __tablename__ = 'students'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    last_name: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    group_id: Mapped[int] = mapped_column(ForeignKey('groups.id'))

    group: Mapped['Group'] = relationship(back_populates='students')
    grades: Mapped[list['Grade']] = relationship(back_populates='student')


class Teacher(Base):
    __tablename__ = 'teachers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)


    last_name: Mapped[str] = mapped_column(String, nullable=False)
    subjects: Mapped[list['Subject']] = relationship(back_populates='teacher')


class Subject(Base):
    __tablename__ = 'subjects'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    teacher_id: Mapped[int] = mapped_column(ForeignKey('teachers.id'))

    teacher: Mapped['Teacher'] = relationship(back_populates='subjects')
    grades: Mapped[list['Grade']] = relationship(back_populates='subject')


class Grade(Base):
    __tablename__ = 'grades'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    student_id: Mapped[int] = mapped_column(ForeignKey('students.id'))
    subject_id: Mapped[int] = mapped_column(ForeignKey('subjects.id'))
    grade: Mapped[int] = mapped_column(Integer, nullable=False)
    date_received: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    student: Mapped['Student'] = relationship(back_populates='grades')
    subject: Mapped['Subject'] = relationship(back_populates='grades')