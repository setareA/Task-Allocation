#!/usr/bin/python

import queue
import random


class ProcessingProgram:

    def __init__(self, ram, cpu, cpu_type):
        self.ram = ram
        self.cpu = cpu
        self.cpu_type = cpu_type

    def print_program_details(self):
        print("ram: ", self.ram)
        print("cpu: ", self.cpu)
        print("cpu_type: ", self.cpu_type)

    def get_ram(self):
        return self.ram

    def get_cpu(self):
        return self.cpu

    def get_cpu_type(self):
        return self.cpu_type


def create_queue(num_of_elements):
    q = queue.Queue()
    for i in range(0, num_of_elements):
        #print("num_of_elements",num_of_elements)
        program = ProcessingProgram(random.randint(1, 128), random.randint(1, 50), random.choice([1, 2]))
        q.put(program)
    return q
