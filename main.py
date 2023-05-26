import csv
import random
import datetime
import tkinter
import uuid
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import logging
import pymysql
import json
from tkinter import *
from tkinter.ttk import Combobox
from subj_reg import subj_reg

res_path = 'E:\\bigdata\\school\\csvfiles\\'
orig_path = 'E:\\bigdata\\school\\csvfiles_orig\\'
engine = create_engine(url=f"mysql+pymysql://alex3@192.168.1.75/school", echo=False)
subj_cfg = 'C:\\Users\\alex\\PycharmProjects\\school\\subj_cfg.csv'
connection = engine.raw_connection()

logfile = f'{res_path}process.log'
log = logging.getLogger("my_log")
log.setLevel(logging.INFO)
FH = logging.FileHandler(logfile, encoding='utf-8')
basic_formater = logging.Formatter('%(asctime)s : [%(levelname)s] : %(message)s')
FH.setFormatter(basic_formater)
log.addHandler(FH)

log.info('start program---------------------------------------------------------------------------')

# def vvod(var_list):
# global menu_results
# list_of_cb_values = []
# for i in range(6):
#     if var_list[i].get() == 1:
#         list_of_cb_values.append(i + 6)
# menu_results = {'class_nums': list_of_cb_values, 'rates_in_day': ratecombo.get()}
# # menu_results = {'daterange': datescombo.get(), 'class': classcombo.get(), 'subj': subjcombo.get(), 'class_nums': list_of_cb_values, 'rates_in_day': ratecombo.get()}
# window.destroy()
def vvod():
    global menu_results
    list_of_cb_values = []
    for i in range(6):
        list_of_cb_values.append(i+6)
    menu_results = {'class_nums': list_of_cb_values}

def generate_students(menu_results):
    log.info('generating students')
    mname_list = []
    fname_list = []
    with open('fsurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            fname_list.append(row.split(' ')[0])
    with open('msurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            mname_list.append(row.split(' ')[0])

    msurname_list = []
    fsurname_list = []
    with open('fsurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            fsurname_list.append(row.split(' ')[1])
    with open('msurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            msurname_list.append(row.split(' ')[1])

    class_letter_list = ['A', 'Б', 'В', 'Г', 'Д']
    class_num_list = menu_results['class_nums']
    class_list = []
    print('generating classes')
    for num in class_num_list:
        for letter in class_letter_list:
            class_list.append(str(num)+str(letter))
    age_list = range(2006, 2009)
    sex_list = ['М', 'Ж']
    result_list = []
    print('generating students')
    for _ in tqdm(range(len(class_list)*30)):
        is_male = random.randint(0, 1)
        result_list.append({'GUID': str(uuid.uuid4()),
                            'name': random.choice(mname_list) if is_male == 1 else random.choice(fname_list),
                            'surname': random.choice(msurname_list) if is_male == 1 else random.choice(fsurname_list),
                            'sex': 'М' if is_male == 1 else 'Ж',
                            'age': random.choice(age_list),
                            'clss': random.choice(class_list)})

    keys = result_list[0].keys()
    with open(res_path + 'students.csv', 'w', newline='', encoding='utf-8') as output_csv:
        dict_writer = csv.DictWriter(output_csv, keys)
        dict_writer.writeheader()
        dict_writer.writerows(result_list)
    return result_list, class_list, class_num_list, class_letter_list


def generate_teachers(subj_list):
    log.info('generating teachers')
    mname_list = []
    fname_list = []
    with open('fsurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            fname_list.append(row.split(' ')[0])
    with open('msurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            mname_list.append(row.split(' ')[0])

    msurname_list = []
    fsurname_list = []
    with open('fsurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            fsurname_list.append(row.split(' ')[1])
    with open('msurnames.txt', 'r', encoding='utf-8') as surnames:
        for row in surnames.readlines():
            msurname_list.append(row.split(' ')[1])
    result_list = []
    print('generating teachers')
    for sub in tqdm(subj_list):
        is_male = random.randint(0, 1)
        result_list.append({'GUID': str(uuid.uuid4()),
                            'name': random.choice(mname_list) if is_male == 1 else random.choice(fname_list),
                            'surname': random.choice(msurname_list) if is_male == 1 else random.choice(fsurname_list),
                            'sex': 'М' if is_male == 1 else 'Ж',
                            'subj': sub[0]})

    keys = result_list[0].keys()
    with open(res_path + 'teachers.csv', 'w', newline='', encoding='utf-8') as output_csv:
        dict_writer = csv.DictWriter(output_csv, keys)
        dict_writer.writeheader()
        dict_writer.writerows(result_list)

def select_all():
    for i in cbuts:
        i.select()

def deselect_all():
    for i in cbuts:
        i.deselect()


def generate_subj(subj_cfg):
    log.info('generating subjects')
    subj_list = []
    with open(subj_cfg, 'r', encoding='utf-8') as data:
        for line in csv.reader(data, delimiter=','):
            subj_list.append(line)
    with open(res_path + 'subjects.csv', 'w', newline='', encoding='utf-8') as output_csv:
        writer = csv.writer(output_csv, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(('subjects', 'quantity'))
        for row in subj_list:
            writer.writerow(row)
    return subj_list

def daterange(start_date, end_date):
    log.info('getting daterange')
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def generate_shedule(students, menu_results, subj_reg):
    class_list = list(students).pop(1)
    class_num_list = list(students).pop(2)
    class_letter_list = list(students).pop(3)
    log.info('generating shedule')
    i = 0
    result_list = []
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM calendar')
    print('generating shedule')
    # if menu_results['rates_in_day'] == '2-3':
    for date in tqdm(cursor.fetchall()):
        if date[1] <= 4:
            for subject in subj_reg[date[1]]:
                for student in students[0]:
                    result_list.append({'ID': i,
                                        'student_id': student["GUID"],
                                        'date': date[0],
                                        'subj': subject,
                                        'rate': random.choice([2, 3, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5])
                                        })
                    i += 1
    # if menu_results['rates_in_day'] == '5':
    #     for date in tqdm(cursor.fetchall()):
    #         if date[1] <= 4:
    #             for student in students[0]:
    #                 for _ in range(5):
    #                     result_list.append({'ID': i,
    #                                         'student_id': student["GUID"],
    #                                         'date': date[0],
    #                                         'subj': random.choice(subj)[0],
    #                                         'rate': random.choice([2, 3, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5])
    #                                         })
    #                     i += 1
    keys = result_list[0].keys()
    with open(res_path + 'shedule.csv', 'w', newline='', encoding='utf-8') as output_csv:
        dict_writer = csv.DictWriter(output_csv, keys)
        dict_writer.writeheader()
        dict_writer.writerows(result_list)
    return class_list, subj, result_list, students[0], class_num_list, class_letter_list


def generate_calendar():
    log.info('generating calendar')
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS calendar")
    cursor.execute("CREATE TABLE calendar"
                   "(date date, weekday smallint, quater varchar(2))")
    print('generating calendar')
    for single_date in tqdm(daterange(start_date=datetime.date(2022, 9, 1), end_date=datetime.date(2023, 5, 31))):
        if single_date in list(daterange(datetime.date(2022, 9, 1), datetime.date(2022, 10, 31))):
            cursor.execute("INSERT INTO calendar VALUES (%s, %s, %s)", (single_date, single_date.weekday(), 'Q1'))
            connection.commit()
        if single_date in list(daterange(datetime.date(2022, 11, 1), datetime.date(2022, 12, 31))):
            cursor.execute("INSERT INTO calendar VALUES (%s, %s, %s)", (single_date, single_date.weekday(), 'Q2'))
            connection.commit()
        if single_date in list(daterange(datetime.date(2023, 1, 1), datetime.date(2023, 3, 31))):
            cursor.execute("INSERT INTO calendar VALUES (%s, %s, %s)", (single_date, single_date.weekday(), 'Q3'))
            connection.commit()
        if single_date in list(daterange(datetime.date(2023, 4, 30), datetime.date(2023, 5, 31))):
            cursor.execute("INSERT INTO calendar VALUES (%s, %s, %s)", (single_date, single_date.weekday(), 'Q4'))
            connection.commit()

def create_cbuts():
    var_list = []
    for index, item in enumerate(cbuts_text):
        var_list.append(IntVar())
        cbuts.append(Checkbutton(window, text=item, variable=var_list[index]))
        cbuts[index].pack()
    return var_list

def calculating(shedule, students, subj_list):
    id = 0
    student_rates_raw = []
    student_rates = []
    quater_list = []
    cursor = connection.cursor()
    cursor.execute('SELECT `GUID`, `name`, `surname`, clss FROM students')
    students_guid = cursor.fetchall()
    for student_guid, name, surname, clss in students_guid:
        cursor.execute(f'SELECT `rate`, `subj`, `date` FROM shedule WHERE `student_id` = "{student_guid}"')
        rates = cursor.fetchall()
        student_rates_raw.append({'GUID': student_guid,
                                  'name': name,
                                  'surname': surname,
                                  'clss': clss,
                                  'rates': rates})
    for student in student_rates_raw:
        print('-')
        for subj in subj_list:
            print()
            for dates in [list(daterange(datetime.date(2022, 9, 1), datetime.date(2022, 10, 31))),
                          list(daterange(datetime.date(2022, 11, 1), datetime.date(2022, 12, 31))),
                          list(daterange(datetime.date(2023, 1, 1), datetime.date(2023, 3, 31))),
                          list(daterange(datetime.date(2023, 4, 30), datetime.date(2023, 5, 31)))]:
                summ = 0
                count = 0
                for rate in student['rates']:
                    if rate[1] == subj[0] and rate[2] in dates:
                        summ += rate[0]
                        count += 1
                quater_list.append(int(round(summ / count, 0)))
            preyear_rate = sum(quater_list) / len(quater_list)
            if str(preyear_rate).split('.')[1] == '5':
                preyear_rate += 0.1
            student_rates.append({'ID': id,
                                  'GUID': student['GUID'],
                                  'name': student['name'],
                                  'surname': student['surname'],
                                  'class': student['clss'],
                                  'subj': subj[0],
                                  'rates_1': quater_list[0],
                                  'rates_2': quater_list[1],
                                  'rates_3': quater_list[2],
                                  'rates_4': quater_list[3],
                                  'year_rates': round(preyear_rate)})
            id += 1
            quater_list.clear()
        keys = student_rates[0].keys()
        with open(res_path + 'rates.csv', 'w', newline='', encoding='utf-8') as output_csv:
            dict_writer = csv.DictWriter(output_csv, keys)
            dict_writer.writeheader()
            dict_writer.writerows(student_rates)

def query(cursor, menu_results):
    if menu_results['daterange'] != 'год':
        cursor.execute(f'SELECT `GUID`, `name`, `surname`, `class`, `rates_{menu_results["daterange"].split(" ")[0]}` '
                       f'FROM `rates` '
                       f'WHERE `subj` = "{menu_results["subj"]}" '
                       f'AND `class` = "{menu_results["class"]}"')
    else:
        cursor.execute(f'SELECT * '
                       f'FROM `rates` '
                       f'WHERE `subj` = "{menu_results["subj"]}" '
                       f'AND `class` = "{menu_results["class"]}"')
    data = cursor.fetchall()
    with open(res_path + 'result.txt', 'w',  encoding='utf-8') as res_file:
        res_file.write(f'Отчёт по параметрам:\n\t'
                       f'   Период - {menu_results["daterange"]}\n\t'
                       f'   Класс - {menu_results["class"]}\n\t'
                       f'   Предмет - {menu_results["subj"]}\n------------------------------\n\n')
        if menu_results['daterange'] != 'год':
            for num, row in enumerate(data):
                surname_kostil = row[2].replace('\n', '')
                res_file.write(f'{num+1}. {row[1]} {surname_kostil} - [{int(row[4])}]\n')
        else:
            for num, row in enumerate(data):
                surname_kostil = row[3].replace('\n', '')
                res_file.write(f'{num+1}. {row[2]} {surname_kostil} - <{int(row[6])}, {int(row[7])}, {int(row[8])}, {int(row[9])}> - [{int(row[10])}]\n')


cursor = connection.cursor()
log.info('delete links')
try:
    cursor.execute('ALTER TABLE shedule DROP FOREIGN KEY shedule_ibfk_1;')
    cursor.execute('ALTER TABLE shedule DROP FOREIGN KEY shedule_ibfk_2;')
    connection.commit()
except:
    print('связей итак нет')


# window = Tk()
# window.title("")
# var = IntVar()
# datescombo = Combobox(window)
# # classcombo = Combobox(window)
# subjcombo = Combobox(window)
# # classcombo['values'] = class_list
# classsetlbl = Label(window, text="класс")
# classsetlbl.grid(column=0, row=0)
# # classcombo.grid(column=0, row=1)
# yearlbl = Label(window, text="период")
# yearlbl.grid(column=0, row=4)
# qalclbl = Label(window, text="предмет")
# qalclbl.grid(column=0, row=6)
# datescombo['values'] = ('1 четверть', '2 четверть', '3 четверть', '4 четверть', 'год')
# datescombo.grid(column=0, row=5)
# subjcombo['values'] = subj_list
# subjcombo.grid(column=0, row=7)
# ratelabel = Label(window, text='количество оценок в день')
# ratecombo = Combobox(window)
# ratecombo['values'] = ['2-3', '5']
# ratelabel.pack()
# ratecombo.pack()
# classlbl = Label(window, text="парралели:")
# classlbl.pack()
# list_cb = []
# cbuts_text = ['6','7','8','9','10','11']
# cbuts = []
# var_list = create_cbuts()
# Button(window, text = 'Все', command = select_all).pack()
# Button(window, text = 'Никакие', command = deselect_all).pack()
# btn = Button(window, text="Ввод", command=vvod(var_list))
# btn.pack()
# window.geometry('500x1000')
# window.mainloop()





log.info('start generating')
vvod()
generate_calendar()
subj = generate_subj(subj_cfg)
result = generate_shedule(generate_students(menu_results), menu_results, subj_reg)
class_list = result[0]
subj_list = result[1]
shedule = result[2]
students = result[3]
generate_teachers(subj_list)
class_num_list = result[4]
class_letter_list = result[5]
log.info('end generating')

cursor.execute('DROP TABLE IF EXISTS classes;')
cursor.execute('CREATE TABLE classes (class text);')
for clss in class_list:
    cursor.execute(f'INSERT INTO classes (class) VALUES ("{clss}")')

cursor.execute('DROP TABLE IF EXISTS class_nums;')
cursor.execute('CREATE TABLE class_nums (num smallint);')
for num in class_num_list:
    cursor.execute(f'INSERT INTO class_nums (num) VALUES ({num})')

cursor.execute('DROP TABLE IF EXISTS class_letters;')
cursor.execute('CREATE TABLE class_letters (letter text);')
for letter in class_letter_list:
    cursor.execute(f'INSERT INTO class_letters (letter) VALUES ("{letter}")')


log.info('push on mysql')
csv_file = pd.read_csv(res_path + 'students.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("students", engine, if_exists='replace', index=False)
csv_file = pd.read_csv(res_path + 'teachers.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("teachers", engine, if_exists='replace', index=False)
csv_file = pd.read_csv(res_path + 'shedule.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("shedule", engine, if_exists='replace', index=False)
csv_file = pd.read_csv(res_path + 'subjects.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("subjects", engine, if_exists='replace', index=False)

log.info('create links')
cursor.execute(
    'ALTER TABLE `students` CHANGE `GUID` `GUID` VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL;')
cursor.execute(
    'ALTER TABLE `shedule` CHANGE `student_id` `student_id` VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL;')
cursor.execute('ALTER TABLE `students` ADD PRIMARY KEY( `GUID`);')
cursor.execute('ALTER TABLE `calendar` ADD PRIMARY KEY( `date`);')
cursor.execute('ALTER TABLE `shedule` ADD PRIMARY KEY( `ID`);')
cursor.execute('ALTER TABLE `shedule` CHANGE `date` `date` DATE NULL DEFAULT NULL;')
cursor.execute(
    'ALTER TABLE `shedule` ADD FOREIGN KEY (`student_id`) REFERENCES `students`(`GUID`) ON DELETE RESTRICT ON UPDATE RESTRICT;')
cursor.execute(
    'ALTER TABLE `shedule` ADD FOREIGN KEY (`date`) REFERENCES `calendar`(`date`) ON DELETE RESTRICT ON UPDATE RESTRICT;')
connection.commit()

calculating(shedule, students, subj_list)
csv_file = pd.read_csv(res_path + 'rates.csv', delimiter=',', on_bad_lines='skip')
csv_file.to_sql("rates", engine, if_exists='replace', index=False)
cursor.execute('ALTER TABLE `rates` ADD PRIMARY KEY( `ID`);')
# query(cursor, menu_results)
log.info('end program')

