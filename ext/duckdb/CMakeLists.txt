cmake_minimum_required(VERSION 3.20)
cmake_policy(SET CMP0148 OLD)
project(blobfilters_duckdb LANGUAGES C CXX)

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
# DuckDB C API headers
#===--------------------------------------------------------------------===

FetchContent_Declare(duckdb_capi
    GIT_REPOSITORY https://github.com/duckdb/extension-template-c.git
    GIT_TAG        main
    GIT_SHALLOW    TRUE
)
FetchContent_Populate(duckdb_capi)
set(DUCKDB_CAPI_INCLUDE "${duckdb_capi_SOURCE_DIR}/duckdb_capi")

#===--------------------------------------------------------------------===
# DuckDB extension
#===--------------------------------------------------------------------===

add_library(roaring_duckdb MODULE ${ROOT}/duckdb_ext/src/roaring_duckdb.c)
target_link_libraries(roaring_duckdb PRIVATE roaring_fp)
target_include_directories(roaring_duckdb PRIVATE
    ${ROOT}/include
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
    COMMAND ${PYTHON_EXE} "${ROOT}/duckdb_ext/append_metadata.py"
        "$<TARGET_FILE:roaring_duckdb>"
        "$<TARGET_FILE_DIR:roaring_duckdb>/blobfilters.duckdb_extension"
    COMMENT "Appending DuckDB extension metadata -> blobfilters.duckdb_extension"
)

install(FILES "${CMAKE_BINARY_DIR}/duckdb/blobfilters.duckdb_extension"
    DESTINATION blobfilters_duckdb/)
