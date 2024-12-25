
cmake_minimum_required(VERSION 3.22)

project(ddb-wal-dump LANGUAGES CXX)

add_subdirectory(duckdb)
add_executable(ddb-wal-dump main.cpp)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

target_link_libraries(ddb-wal-dump PRIVATE duckdb_static)
set_target_properties(ddb-wal-dump PROPERTIES CXX_STANDARD 17)

include(FetchContent)

FetchContent_Declare(
    json 
    URL 
    https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)
FetchContent_MakeAvailable(json)

target_link_libraries(ddb-wal-dump PRIVATE nlohmann_json::nlohmann_json)
