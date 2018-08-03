#!/usr/bin/python

import sys
import database
import sqlite3
import program
import random
from shutil import copyfile


def first_fit(available_id_cc, ram, cpu, type_of_cpu, filename):

    final_answer_id = -1
    final_answer_foreign_id, selected_cpu, selected_ram, type = [-1, -1, -1, -1]
    ccc = sqlite3.connect(filename)

    found = 0
    for id_cc in available_id_cc:
        if found == 0:
            counter = 0
            for row in ccc.execute("SELECT * FROM NODES WHERE ram > " + str(ram) + " AND cpu > " + str(cpu)+" AND CC_ID = "+str(id_cc)+" AND CPU_TYPE = "+str(type_of_cpu)):
                if counter == 0:
                    final_answer_id = row[0]
                    final_answer_foreign_id = row[1]
                    selected_ram = row[2]
                    selected_cpu = row[3]
                    type = row[4]

                    counter += 1
                    found = 1
                    break
            if found == 1:
                break

    ccc.execute('''UPDATE NODES SET ram = ram - ?, cpu = cpu - ? WHERE id = ?''', (ram,cpu,final_answer_id))
    ccc.execute('''UPDATE COMPUTING_CENTERS SET ram = ram - ?, cpu = cpu - ? WHERE id = ?''', (ram, cpu, final_answer_foreign_id))

    ccc.commit()
    ccc.close()
    print_report(final_answer_id, final_answer_foreign_id, ram, cpu, type_of_cpu, type, selected_ram, selected_cpu,'first_fit.txt')


def best_fit(available_id_cc, ram, cpu, type_of_cpu, filename):
    final_answer_id = -1
    final_answer_foreign_id, selected_cpu, selected_ram, type = [-1, -1, -1, -1]
    cc = sqlite3.connect(filename)

    counter = 0
    for id_cc in available_id_cc:

        for row in cc.execute("SELECT * FROM NODES WHERE ram > " + str(ram) + " AND cpu > " + str(
                cpu) + " AND CC_ID = " + str(id_cc) + " AND CPU_TYPE = " + str(type_of_cpu)):
            if counter == 0:
                min = row[2] + row[3]
                final_answer_id = row[0]
                final_answer_foreign_id = row[1]
                selected_ram = row[2]
                selected_cpu = row[3]
                type = row[4]
                counter += 1
            elif (row[2] + row[3]) < min:
                min = row[2] + row[3]
                final_answer_id = row[0]
                final_answer_foreign_id = row[1]
                selected_ram = row[2]
                selected_cpu = row[3]
                type = row[4]

    cc.execute('''UPDATE NODES SET ram = ram - ?, cpu = cpu - ? WHERE id = ?''', (ram, cpu, final_answer_id))
    cc.execute('''UPDATE COMPUTING_CENTERS SET ram = ram - ? AND cpu = cpu - ? WHERE id = ?''',
                (ram, cpu, final_answer_foreign_id))

    cc.commit()
    cc.close()
    print_report(final_answer_id, final_answer_foreign_id, ram, cpu, type_of_cpu, type, selected_ram, selected_cpu,'best_fit.txt')


def worst_fit(available_id_cc, ram, cpu, type_of_cpu, filename):
    final_answer_id = -1
    final_answer_foreign_id, selected_cpu, selected_ram, type = [-1, -1, -1, -1]
    cworst = sqlite3.connect(filename)

    counter = 0
    for id_cc in available_id_cc:

        for row in cworst.execute("SELECT * FROM NODES WHERE ram > " + str(ram) + " AND cpu > " + str(
                cpu) + " AND CC_ID = " + str(id_cc) + " AND CPU_TYPE = " + str(type_of_cpu)):
            if counter == 0:
                max = row[2] + row[3]
                final_answer_id = row[0]
                final_answer_foreign_id = row[1]
                selected_ram = row[2]
                selected_cpu = row[3]
                type = row[4]
                counter += 1
            elif (row[2] + row[3]) > max:
                max = row[2] + row[3]
                final_answer_id = row[0]
                final_answer_foreign_id = row[1]
                selected_ram = row[2]
                selected_cpu = row[3]
                type = row[4]

    cworst.execute('''UPDATE NODES SET ram = ram - ?, cpu = cpu - ? WHERE id = ?''', (ram, cpu, final_answer_id))
    cworst.execute('''UPDATE COMPUTING_CENTERS SET ram = ram - ?, cpu = cpu - ? WHERE id = ?''',
                (ram, cpu, final_answer_foreign_id))

    cworst.commit()
    cworst.close()
    print_report(final_answer_id, final_answer_foreign_id, ram, cpu, type_of_cpu, type, selected_ram, selected_cpu,'worst_fit.txt')


def print_report(final_answer_id, final_answer_foreign_id, ram, cpu, type_of_cpu, type, selected_ram, selected_cpu,algorithm_type):
    if final_answer_id == -1:
        report = "Processing program -> ram : " + str(ram) + " cpu : " + str(cpu) + " ,cpu_type : " + str(
            type_of_cpu) + " -> NOT ENOUGH RESOURCE "
    else:
        report = """Processing program -> ram : """ + str(ram) + """ ,cpu : """ + str(cpu) + " ,cpu_type : " + str(
            type_of_cpu) + """\nCOMPUTING_CENTER ->  id : """ \
                 + str(final_answer_foreign_id) + """\nNODE ->  id : """ \
                 + str(final_answer_id) + " ,available ram : """ + str(
            selected_ram - ram) + """ ,available cpu : """ + str(selected_cpu - cpu) + " ,cpu_type : " + str(type)

    with open(algorithm_type, 'a') as out:
        out.write(report + '\n\n' + "#################" + '\n\n')


def main():

    filename = input()
    connection = database.ComputingCenters(filename)
    connection.create_tables()
    connection.insert_db()

    copyfile(filename, 'f.db')
    copyfile(filename, 'w.db')

    c = sqlite3.connect(filename)
    c1 = sqlite3.connect('f.db')
    c2 = sqlite3.connect('w.db')

    programs_queue = program.create_queue(random.randint(10, 40))

    while programs_queue.empty() == False:
        available_id_cc = []
        available_id_cc1 = []
        available_id_cc2 = []

        processing_program = programs_queue.get()
        r = processing_program.get_ram()
        cp = processing_program.get_cpu()
        cpu_type = processing_program.get_cpu_type()


        for row in c.execute("SELECT ID FROM COMPUTING_CENTERS WHERE ram > " + str(r) + " AND cpu > " + str(cp)):
            available_id_cc.append(row[0])
        best_fit(available_id_cc, r, cp, cpu_type, filename)

        for row in c1.execute("SELECT ID FROM COMPUTING_CENTERS WHERE ram > " + str(r) + " AND cpu > " + str(cp)):
            available_id_cc1.append(row[0])
        first_fit(available_id_cc1, r, cp, cpu_type, 'f.db')

        for row in c2.execute("SELECT ID FROM COMPUTING_CENTERS WHERE ram > " + str(r) + " AND cpu > " + str(cp)):
            available_id_cc2.append(row[0])
        worst_fit(available_id_cc2, r, cp, cpu_type, 'w.db')

    c.close()
    c1.close()
    c2.close()

if __name__ == "__main__":
    main()
