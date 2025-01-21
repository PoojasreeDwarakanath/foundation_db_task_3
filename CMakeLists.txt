cmake_minimum_required(VERSION 3.30)
project(Task3 C)

set(CMAKE_C_STANDARD 17)

add_library(Task3 STATIC library.c
        basic_ops.c
        basic_ops.h
        fdb_ex.c
        fdb_ex.h)
