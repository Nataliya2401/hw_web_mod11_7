from sqlalchemy import func, desc, and_, select

from src.models import Grade, Group, Discipline, Teacher, Student
from src.db import session
from pprint import pprint


def select_1():
    """
    --Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_2(discipine_id: int):
    """
    --Знайти студента із найвищим середнім балом з певного предмета.
    """
    result = session.query(Discipline.name, Student.fullname, func.round(func.avg(Grade.grade), 2) \
                           .label('avg_grade')).select_from(Grade).join(Student).join(Discipline) \
        .filter(Discipline.id == discipine_id).group_by(Student.id, Discipline.name).order_by(desc('avg_grade')) \
        .limit(2).all()
    return result


def select_3(discipline_id: int):
    # Знайти середній бал у групах з певного предмета.
    result = session.query(Group.name, Discipline.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).join(Discipline).join(Group).filter(Discipline.id == discipline_id) \
        .group_by(Group.id, Discipline.id).order_by(Group.id).all()
    return result


def select_4():
    # 4 : Знайти середній бал на потоці (по всій таблиці оцінок).
    result = session.query(func.round(func.avg(Grade.grade), 3).label('avg_grade')).scalar()
    return result


def select_5(teacher_id: int):
    # 5 : Знайти які курси читає певний викладач
    result = session.query(Teacher.fullname, Discipline.name).select_from(Teacher).join(Discipline, isouter=True) \
        .filter(Teacher.id == teacher_id).all()
    return result


def select_6(group_id: int):
    # 6 : Знайти список студентів у певній групі.
    result = session.query(Group.id, Group.name, Student.fullname).select_from(Student).join(Group) \
        .filter(Group.id == group_id).order_by(Student.fullname).all()
    return result


def select_7(group_id: int, discipline_id: int):
    # 7 : Знайти оцінки студентів у окремій групі з певного предмета.
    result = session.query(Group.id, Group.name, Discipline.name, Student.fullname, Grade.grade) \
        .select_from(Grade).join(Discipline).join(Student).join(Group)\
        .filter(and_(Discipline.id == discipline_id),(Group.id == group_id)).order_by(Student.fullname).all()
    return result


def select_7n(group_id: int, discipline_id: int):
    # 7 : Знайти оцінки студентів у окремій групі з певного предмета.
    result = session.query(Student.group_id, Student.fullname, Grade.grade).select_from(Grade)\
        .join(Student).filter(and_((Student.group_id == group_id), (Grade.discipline_id == discipline_id)))\
        .order_by(Student.fullname).all()
    return result


def select_8(teacher_id: int):
    # 8 : Знайти середній бал, який ставить певний викладач зі своїх предметів.
    result = session.query(Teacher.fullname, Discipline.name, func.round(func.avg(Grade.grade), 3) \
                           .label('avg_grade')).select_from(Teacher).join(Discipline, isouter=True) \
        .join(Grade, isouter=True).filter(Teacher.id == teacher_id).group_by(Discipline.id, Teacher.id).all()
    return result


def select_9s(student_id:int):
    # 9 : Знайти список курсів, які відвідує студент.
    result = session.query(Student.fullname, Discipline.name).select_from(Grade) \
        .join(Student).filter(Student.id == student_id).group_by(Student.id, Discipline.name).all()
    return result


def select_9(student_id:int):
    # 9 : Знайти список курсів, які відвідує студент.
    result = session.query(Discipline.name).join(Grade) \
        .filter(Grade.student_id == student_id).group_by(Discipline.name).all()
    return result


def select_10(student_id:int, teacher_id:int):
    # 10:Список курсів, які певному студенту читає певний викладач.
    result = session.query(Discipline.name, Student.fullname, Teacher.fullname).select_from(Grade)\
        .join(Discipline, isouter=True).join(Student, isouter=True).join(Teacher, isouter=True)\
        .filter(Student.id == student_id, Teacher.id == teacher_id).group_by(Student.id, Discipline.id, Teacher.id).all()
    return result


def select_10n(student_id:int, teacher_id:int):
    # 10:Список курсів, які певному студенту читає певний викладач.
    result = session.query(Discipline.name).join(Grade)\
        .filter(and_(Grade.student_id == student_id), (Discipline.teacher_id == teacher_id))\
        .group_by(Discipline.id).all()
    return result


def select_11n(student_id:int, teacher_id:int):
    # 11:Середній балл, який конкретний викладач ставить конкретному студенту
    result = session.query(func.round(func.avg(Grade.grade), 1).label('average_grade'))\
        .join(Discipline)\
        .filter(and_(Grade.student_id == student_id, Discipline.teacher_id == teacher_id)).all()

    return result


def select_11(student_id:int, teacher_id:int):
    # 11:Середній балл, який конкретний викладач ставить конкретному студенту - по предметно
    result = session.query(Discipline.name, Teacher.fullname, func.round(func.avg(Grade.grade), 1).label('agg_grade'))\
        .select_from(Teacher)\
        .join(Discipline, isouter=True)\
        .join(Grade, isouter=True)\
        .filter(and_(Grade.student_id == student_id, Teacher.id == teacher_id))\
        .group_by(Discipline.id, Teacher.id).all()
    return result


def select_12(group_id:int, discipline_id:int):
    # 12: Оцінки студентів в групі по дисципліні на останньому занятті
    subquery = (select(func.max(Grade.date_of)).join(Student).join(Group)\
        .where(and_(Group.id == group_id, Grade.discipline_id == discipline_id)).scalar_subquery())
    result = session.query(Grade.date_of, Group.name, Student.fullname, Discipline.name, Grade.grade)\
        .select_from(Grade) \
        .join(Student, isouter=True) \
        .join(Discipline, isouter=True) \
        .join(Group, isouter=True) \
        .filter((Group.id == group_id), (Discipline.id == discipline_id), (Grade.date_of == subquery)).all()

    return result


if __name__ == '__main__':
    print("1. 5 студентів із найбільшим середнім балом з усіх предметів")
    pprint(select_1())
    print("2. Cтудент із найвищим середнім балом з певного предмета (discipline #4)")
    pprint(select_2(4))
    print("3. середній бал у групах з певного предмета (discipline #4)")
    pprint(select_3(4))
    print("4. Знайти середній бал на потоці (по всій таблиці оцінок)")
    print(select_4())
    print("5 : Знайти які курси читає певний викладач")
    pprint(select_5(4))
    print("6 : Знайти список студентів у певній групі.")
    pprint(select_6(3))
    print("7 : Знайти оцінки студентів у окремій групі з певного предмета")
    pprint(select_7n(3, 2))
    print("8 : Знайти середній бал, який ставить певний викладач зі своїх предметів.")
    pprint(select_8(5))
    print("9 :Знайти список курсів, які відвідує студент.")
    pprint(select_9(6))
    print("10:Список курсів, які певному студенту читає певний викладач.")
    # pprint(select_10(6, 5))
    print("11:Середній балл, який конкретний викладач ставить конкретному студенту - по предметно.")
    pprint(select_11(6, 1))
    print("12:Оцінки студентів в групі по дисципліні на останньому занятті")
    pprint(select_12(4, 1))
