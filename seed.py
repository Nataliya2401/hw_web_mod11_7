from datetime import date, datetime, timedelta
from random import randint, choice
from pprint import pprint
from faker import Faker
from sqlalchemy import select

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


disciplines = [
    "Теорія ймовірності",
    "Математиний аналіз",
    "Аналітична геометрія",
    "Диф рівняння",
    "English",
    "Algebra",
    "History"
]

groups = ['TT1', 'TT2', 'VV-1', 'VV-2']
NUMBER_TEACHERS = 5
NUMBER_STUDENTS = 50
fake = Faker()


def get_list_dates(start_date: date, end_date: date) -> list:
    list_dates = []
    current_d = start_date
    while current_d <= end_date:
        if current_d.isoweekday() < 6:
            list_dates.append(current_d)
        current_d += timedelta(1)
    return list_dates


def seed_teachers():
    for _ in range(NUMBER_TEACHERS):
        teacher = Teacher(fullname=fake.name())
        session.add(teacher)
    session.commit()


def seed_disciplines():
    teachers = session.query(Teacher).all()
    for d in disciplines:
        teacher = choice(teachers)
        discipline = Discipline(name=d, teacher_id=teacher.id)
        session.add(discipline)
    session.commit()


def seed_groups():
    for group in groups:
        session.add(Group(name=group))
    session.commit()


def seed_students():
    groups = session.query(Group).all()
    for _ in range(NUMBER_STUDENTS):
        group = choice(groups)
        student = Student(fullname=fake.name(), group_id=group.id)
        session.add(student)
    session.commit()


def seed_grades():
    start_education = datetime.strptime('2022-09-01', "%Y-%m-%d")
    end_education = datetime.strptime('2023-05-31', "%Y-%m-%d")
    list_dates = get_list_dates(start_education, end_education)
    disciplines = session.query(Discipline).all()

    for day in list_dates:
        random_discipline = choice(disciplines)
        random_students = [randint(1, NUMBER_STUDENTS) for _ in range(7)]
        for st in random_students:
            grade = Grade(grade=randint(1, 12), student_id=st, discipline_id=random_discipline.id, date_of=day.date())
            session.add(grade)
    session.commit()


if __name__ == '__main__':
    seed_teachers()
    seed_disciplines()
    seed_groups()
    seed_students()
    seed_grades()

