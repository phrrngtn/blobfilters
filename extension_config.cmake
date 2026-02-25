duckdb_extension_load(roaring
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/duckdb_ext/src
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/duckdb_ext/src/include
    LINKED_LIBS "roaring_fp"
)
