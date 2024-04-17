from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session

def query_1():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
            .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def query_2():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result

def query_3():
    """
    select
	    s2.name,
	    s.group_id,
	    round(avg(g.grade),2) as avg_grade
    from grades g
    join students s on g.student_id = s.id
    join subjects s2 on g.subject_id = s2.id
    where s2.name = 'ручка' and s.group_id = 1
    group by 1,2
    """
    result = session.query(Subject.name, Student.group_id, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).join(Subject).fllter(Subject.name=='ручка').group_by(Subject.name, Student.group_id)\
        .all()
    return result

def query_4():
    """
    --4. Знайти середній бал на потоці (по всій таблиці оцінок)
    select
	    round(avg(g.grade), 2) as avg_grade
    from grades g;
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade)
    return result

def query_5():
    """
    --5. Знайти які курси читає певний викладач
    select *
    from teachers t
    join subjects s on t.teacher_id = s.teacher_id
    where t.fullname = 'Мілена Яремчук';
    """
    result = session.query(Teacher.id, Teacher.fullname).select_from(Teacher).join(Subject).filter_by(Teacher.fullname=='Мілена Яремчук').all()
    return result

def query_6():
    """
    --6. Знайти всі курси, що викладують викладачі <NAME>
    6. Знайти список студентів у певній групі.
    select *
    from students s
    where group_id = 2
    """
    result = session.query(Student.id, Student.fullname, Student.group_id).select_from(Student).filter_by(Group.id == 2).all()
    return result

def query_7():
    """
    --7. Знайти оцінки студентів у окремій групі з певного предмета.
    select
        s2.name,
        g.grade,
        s.group_id,
        s.fullname
    from grades g
    join students s on g.student_id = s.id
    join subjects s2 on g.subject_id = s2.id
    where s2.name = 'ручка' and s.group_id = 2
    group by 1,2,3,4
    """
    result = session.query(Subject.name, Grade.grade, Student.group_id, Student.fullname) \
            .select_from(Grade).join(Student).join(Subject) \
            .filter_by(and_(Subject.name=='ручка', Student.group_id==2)) \
            .group_by(Subject.name, Grade.grade, Student.group_id, Student.fullname).all()
    return result

def query_8()
    """
    --8. Знайти середній бал, який ставить певний викладач зі своїх предметів
    select
        t.fullname,
        round(avg(grade),2) as avg_grade 
    from teachers t  
    join subjects s on t.teacher_id = s.teacher_id
    join grades g on s.id = g.subject_id 
    where t.fullname like ('%ри%')
    group by 1
    """
    result = session.query(Teacher.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Teacher).join(Subject).join(Grade).fllter(Teacher.fullname=='Орина Фесенко')\
        .group_by(Teacher.fullname).all()
    return result

def query_9():
    """
   --9 Знайти список курсів, які відвідує студент
    select
        s.name,
        s2.name
    from students s2
    join grades g2 on s2.id = g2.student_id
    join subjects s on g2.subject_id = subject_id
    where s2.name = 'Онисим Чумаченко'
    group by s.name, s2.name
    """
    result = session.query(Subject.name, Student.fullname) \
        .select_from(Teacher).join(Subject).join(Grade).fllter(Student.fullname=='Онисим Чумаченко')\
        .group_by(Subject.name, Student.fullname).all()
    return result

def query_10():
    """
    --10 Список курсів, які певному студенту читає певний викладач
    select
        s2.name,
        t.fullname,
        s.name as subject_name
    from students s2
    join grades g on s2.id = g.student_id
    join subjects s on g.subject_id = s.id
    join teachers t on s.id = t.teacher_id
    where s2.name like ('%З%') and t.fullname ilike ('%Фесенко')
    group by 1,2,3
    """
    result = session.query(Student.fullname, Teacher.fullname, Subject.name.label('subject_name')) \
        .select_from(Student).join(Grade).join(Subject).join(Teacher)\
        .filter(and_(Student.fullname=='Онисим Чумаченко', Teacher.fullname == 'Орина Фесенко')\
        .group_by(Student.fullname, Teacher.fullname, Subject.name.label('subject_name')).all()
    return result

def query_11():
    """
    --Додаткове
    --1. Середній бал, який певний викладач ставить певному студентові
    select
        s2.name,
        t.fullname,
        s.name as subject_name,
        round(avg(g.grade),2) as avg_grades
    from students s2
    join grades g on s2.id = g.student_id
    join subjects s on g.subject_id = s.id
    join teachers t on s.id = t.teacher_id
    where s2."name" like ('%З%') and t.fullname ilike ('%Фесенко')
    group by 1,2,3
    """
    result = session.query(Student.fullname, Teacher.fullname, Subject.name.label('subject_name'), func.round(func.avg(Grade.grade), 2)) \
            .select_from(Student).join(Grade).join(Subject).join(Teacher)\
            .filter(and_(Student.fullname=='Онисим Чумаченко', Teacher.fullname == 'Орина Фесенко')\
            .group_by(Student.fullname, Teacher.fullname, Subject.name.label('subject_name')).all()
    return result
def query_12():
    """
    --Додаткове
    --2. Оцінки студентів у певній групі з певного предмета на останньому занятті
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subjects_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result
if __name__ == '__main__':
    print(query_1())
    print(query_2())
    print(query_3())
    print(query_4())
    print(query_5())
    print(query_6())
    print(query_7())
    print(query_8())
    print(query_9())
    print(query_10())
    print(query_11())
    print(query_12())
