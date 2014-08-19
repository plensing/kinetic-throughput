cmake_minimum_required(VERSION 2.8.6)
project(throughput CXX C)

set(CMAKE_CXX_FLAGS "-g --std=c++11 -Wall -Wextra -Werror -Wno-unknown-warning-option -Wno-unused-parameter -Wno-unused-local-typedefs -Wno-null-dereference -DGTEST_USE_OWN_TR1_TUPLE=1 -D__STDC_FORMAT_MACROS")

set(KINETIC_CLIENT_PATH  "$ENV{HOME}/git/kinetic-cpp-client")
set(KINETIC_CLIENT_BUILD "${KINETIC_CLIENT_PATH}/build")

if(EXISTS ${KINETIC_CLIENT_PATH} AND EXISTS ${KINETIC_CLIENT_BUILD})
   message("Found existing kinetic-cpp-client at ${KINETIC_CLIENT_PATH}")
else()
   message(FATAL_ERROR "Existing kinetic-cpp-client not found. Enter correct path.")
endif()

include_directories(
    ${KINETIC_CLIENT_PATH}/vendor/include
    ${KINETIC_CLIENT_PATH}/src/main/generated
    ${KINETIC_CLIENT_PATH}/include
)

set(LIBRARY_DEPENDENCIES
    ${KINETIC_CLIENT_BUILD}/libkinetic_client.a
    ${KINETIC_CLIENT_BUILD}/vendor/lib/libglog.a
    ${KINETIC_CLIENT_BUILD}/vendor/lib/libgflags.a
    ${KINETIC_CLIENT_BUILD}/vendor/lib/libssl.a
    ${KINETIC_CLIENT_BUILD}/vendor/lib/libcrypto.a
    ${KINETIC_CLIENT_BUILD}/vendor/lib/libprotobuf.a

    
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
     threadsafe_blocking_connection.cc
)
add_dependencies(throughput
    kinetic_cpp_client
)
target_link_libraries(throughput
    ${LIBRARY_DEPENDENCIES}
)

