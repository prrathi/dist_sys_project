cmake_minimum_required(VERSION 3.10)
set(CMAKE_CXX_STANDARD 17)
project(HYDFS)

find_package(Protobuf CONFIG REQUIRED)
find_package( gRPC CONFIG REQUIRED)

# Define source and header files
set(SOURCES
  src/main.cpp
  src/common.cpp
  src/worker.cpp
  src/listener.cpp
  src/talker.cpp
  src/hydfs.cpp
  src/utils.cpp
  src/lru_cache.cpp
  src/hydfs_server.cpp
  src/file_transfer_client.cpp
)

set(HEADERS
  includes/common.h
  includes/worker.h
  includes/listener.h
  includes/talker.h
  includes/hydfs.h
  includes/utils.h
  includes/lru_cache.h
  includes/hydfs.grpc.pb.h
  includes/hydfs.pb.h
  includes/hydfs_server.h
  includes/file_transfer_client.h
)

include_directories(includes)

# Define executable
add_executable(${PROJECT_NAME} ${SOURCES} ${HEADERS} src/hydfs.grpc.pb.cc src/hydfs.pb.cc)

# Define dependencies
target_link_libraries(${PROJECT_NAME} pthread gRPC::grpc++)

# Define compiler flags
target_compile_options(${PROJECT_NAME} PRIVATE -Wall -Wextra)

# # Define debug flags
# set(CMAKE_BUILD_TYPE Debug)
# target_compile_options(${PROJECT_NAME} PRIVATE -DDEBUG)

