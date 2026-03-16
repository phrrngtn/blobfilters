cmake_minimum_required(VERSION 3.14)
project(roaring_fp VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

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
# Core library (always built)
#===--------------------------------------------------------------------===

add_subdirectory(src)

#===--------------------------------------------------------------------===
# Python bindings (optional)
#===--------------------------------------------------------------------===

option(BUILD_PYTHON_BINDINGS "Build Python nanobind module" OFF)
if(BUILD_PYTHON_BINDINGS)
    find_package(Python 3.8 REQUIRED COMPONENTS Interpreter Development.Module)

    FetchContent_Declare(
        nanobind
        GIT_REPOSITORY https://github.com/wjakob/nanobind.git
        GIT_TAG        v2.4.0
        GIT_SHALLOW    TRUE
    )
    FetchContent_MakeAvailable(nanobind)

    nanobind_add_module(_core python/bindings.cpp)
    target_link_libraries(_core PRIVATE roaring_fp)
    target_include_directories(_core PRIVATE ${CMAKE_SOURCE_DIR}/include)

    install(TARGETS _core LIBRARY DESTINATION blobfilters)
endif()

#===--------------------------------------------------------------------===
# SQLite extension (optional)
#===--------------------------------------------------------------------===

option(BUILD_SQLITE_EXTENSION "Build SQLite loadable extension" OFF)
if(BUILD_SQLITE_EXTENSION)
    # Download SQLite amalgamation headers
    set(SQLITE_VERSION "3480000")
    set(SQLITE_URL "https://www.sqlite.org/2025/sqlite-amalgamation-${SQLITE_VERSION}.zip")

    FetchContent_Declare(
        sqlite3_headers
        URL ${SQLITE_URL}
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    FetchContent_MakeAvailable(sqlite3_headers)

    add_library(roaring_sqlite MODULE sqlite_ext/src/roaring_sqlite.cpp)
    target_link_libraries(roaring_sqlite PRIVATE roaring_fp)
    target_include_directories(roaring_sqlite PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${sqlite3_headers_SOURCE_DIR}
    )
    set_target_properties(roaring_sqlite PROPERTIES
        PREFIX ""
        OUTPUT_NAME "roaring"
    )

    if(APPLE)
        target_link_options(roaring_sqlite PRIVATE -undefined dynamic_lookup)
    endif()

    # Statically-linked SQLite demo (embeds sqlite3.c so no .load needed)
    add_executable(sqlite_demo
        demo/sqlite_demo.c
        sqlite_ext/src/roaring_sqlite.cpp
        ${sqlite3_headers_SOURCE_DIR}/sqlite3.c
    )
    target_link_libraries(sqlite_demo PRIVATE roaring_fp)
    target_include_directories(sqlite_demo PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${sqlite3_headers_SOURCE_DIR}
    )
    target_compile_definitions(sqlite_demo PRIVATE
        SQLITE_CORE              # We're embedding sqlite, not loading as extension
    )
endif()

#===--------------------------------------------------------------------===
# DuckDB C API headers (for extension build)
#===--------------------------------------------------------------------===

FetchContent_Declare(duckdb_capi
    GIT_REPOSITORY https://github.com/duckdb/extension-template-c.git
    GIT_TAG        main
    GIT_SHALLOW    TRUE
)
FetchContent_Populate(duckdb_capi)
set(DUCKDB_CAPI_INCLUDE "${duckdb_capi_SOURCE_DIR}/duckdb_capi")

#===--------------------------------------------------------------------===
# DuckDB extension (optional — pure C API, no DuckDB install needed)
#===--------------------------------------------------------------------===

option(BUILD_DUCKDB_EXTENSION "Build DuckDB loadable extension" OFF)
if(BUILD_DUCKDB_EXTENSION)
    add_library(roaring_duckdb MODULE duckdb_ext/src/roaring_duckdb.c)
    target_link_libraries(roaring_duckdb PRIVATE roaring_fp)
    target_include_directories(roaring_duckdb PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${DUCKDB_CAPI_INCLUDE}
    )
    set_target_properties(roaring_duckdb PROPERTIES
        PREFIX ""
        OUTPUT_NAME "blobfilters"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/duckdb"
    )
    if(APPLE)
        target_link_options(roaring_duckdb PRIVATE -undefined dynamic_lookup)
    endif()

    # Generate .duckdb_extension with metadata footer
    find_program(PYTHON_EXE NAMES python3 python REQUIRED)
    add_custom_command(TARGET roaring_duckdb POST_BUILD
        COMMAND ${PYTHON_EXE} "${CMAKE_SOURCE_DIR}/duckdb_ext/append_metadata.py"
            "$<TARGET_FILE:roaring_duckdb>"
            "$<TARGET_FILE_DIR:roaring_duckdb>/blobfilters.duckdb_extension"
        COMMENT "Appending DuckDB extension metadata -> blobfilters.duckdb_extension"
    )
endif()

#===--------------------------------------------------------------------===
# Tests and demo
#===--------------------------------------------------------------------===

option(BUILD_TESTS "Build test programs" ON)
if(BUILD_TESTS)
    add_executable(test_roaring test/test_roaring.c)
    target_link_libraries(test_roaring roaring_fp)
    target_include_directories(test_roaring PRIVATE ${CMAKE_SOURCE_DIR}/include)

    add_executable(fingerprint_demo demo/fingerprint_demo.c)
    target_link_libraries(fingerprint_demo roaring_fp)
    target_include_directories(fingerprint_demo PRIVATE ${CMAKE_SOURCE_DIR}/include)
endif()
