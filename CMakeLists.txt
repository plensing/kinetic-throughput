cmake_minimum_required(VERSION 2.8.6)
project(throughput CXX C)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})
find_package(kinetic-c++ REQUIRED)

set(CMAKE_CXX_FLAGS "-g --std=c++11 -Wall")

include_directories(
    ${KINETIC-C++_INCLUDE_DIRS}
)

set(LIBRARY_DEPENDENCIES
    ${KINETIC-C++_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    dl
)

if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64")
else()
    LIST(APPEND LIBRARY_DEPENDENCIES pthread)
endif(APPLE)

add_executable(throughput
     throughput.cc
)
target_link_libraries(throughput
    ${LIBRARY_DEPENDENCIES}
)

