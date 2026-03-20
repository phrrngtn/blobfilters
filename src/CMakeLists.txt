# Core library: libroaring_fp

add_library(roaring_fp STATIC
    roaring_fp.cpp
    ${CMAKE_SOURCE_DIR}/third_party/roaring/roaring.c
)

target_include_directories(roaring_fp PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/third_party/roaring
)

# nlohmann/json and utf8proc are fetched by the root CMakeLists.txt
target_link_libraries(roaring_fp PUBLIC nlohmann_json::nlohmann_json utf8proc)

set_target_properties(roaring_fp PROPERTIES
    C_STANDARD 11
    CXX_STANDARD 17
    POSITION_INDEPENDENT_CODE ON
)
