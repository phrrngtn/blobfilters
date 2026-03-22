cmake_minimum_required(VERSION 3.20)
cmake_policy(SET CMP0148 OLD)
project(blobfilters_sqlite LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(ROOT "${CMAKE_CURRENT_SOURCE_DIR}")

include(FetchContent)

#===--------------------------------------------------------------------===
# nlohmann/json
#===--------------------------------------------------------------------===

FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.11.3
    GIT_SHALLOW    TRUE
)
set(JSON_BuildTests OFF CACHE INTERNAL "")
FetchContent_MakeAvailable(nlohmann_json)

#===--------------------------------------------------------------------===
# utf8proc (Unicode normalization)
#===--------------------------------------------------------------------===

FetchContent_Declare(
    utf8proc
    GIT_REPOSITORY https://github.com/JuliaStrings/utf8proc.git
    GIT_TAG        v2.9.0
    GIT_SHALLOW    TRUE
)
set(UTF8PROC_INSTALL OFF CACHE INTERNAL "")
set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)
FetchContent_MakeAvailable(utf8proc)
unset(CMAKE_POLICY_VERSION_MINIMUM CACHE)

#===--------------------------------------------------------------------===
# SQLite amalgamation headers
#===--------------------------------------------------------------------===

set(SQLITE_VERSION "3480000")
set(SQLITE_URL "https://www.sqlite.org/2025/sqlite-amalgamation-${SQLITE_VERSION}.zip")

FetchContent_Declare(
    sqlite3_headers
    URL ${SQLITE_URL}
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(sqlite3_headers)

#===--------------------------------------------------------------------===
# Core library (roaring_fp)
#===--------------------------------------------------------------------===

add_library(roaring_fp STATIC
    ${ROOT}/src/roaring_fp.cpp
    ${ROOT}/third_party/roaring/roaring.c
)
target_include_directories(roaring_fp PUBLIC
    ${ROOT}/include
    ${ROOT}/third_party/roaring
)
target_link_libraries(roaring_fp PUBLIC nlohmann_json::nlohmann_json utf8proc)
set_target_properties(roaring_fp PROPERTIES
    C_STANDARD 11
    CXX_STANDARD 17
    POSITION_INDEPENDENT_CODE ON
)

#===--------------------------------------------------------------------===
# SQLite extension
#===--------------------------------------------------------------------===

add_library(roaring_sqlite MODULE ${ROOT}/sqlite_ext/src/roaring_sqlite.cpp)
target_link_libraries(roaring_sqlite PRIVATE roaring_fp)
target_include_directories(roaring_sqlite PRIVATE
    ${ROOT}/include
    ${sqlite3_headers_SOURCE_DIR}
)
set_target_properties(roaring_sqlite PROPERTIES
    PREFIX ""
    OUTPUT_NAME "roaring"
)
if(APPLE)
    target_link_options(roaring_sqlite PRIVATE -undefined dynamic_lookup)
    add_custom_command(TARGET roaring_sqlite POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            roaring.so "$<TARGET_FILE_DIR:roaring_sqlite>/roaring.dylib")
endif()

install(TARGETS roaring_sqlite LIBRARY DESTINATION blobfilters_sqlite/)
if(APPLE)
    install(CODE "
        execute_process(COMMAND \${CMAKE_COMMAND} -E create_symlink
            roaring.so roaring.dylib
            WORKING_DIRECTORY \${CMAKE_INSTALL_PREFIX}/blobfilters_sqlite)
    ")
endif()
