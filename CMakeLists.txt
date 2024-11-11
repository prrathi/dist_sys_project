cmake_minimum_required(VERSION 3.10)
project(SWIM)

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Define source and header files
set(SOURCES
  src/main.cpp
  src/common.cpp
  src/worker.cpp
  src/listener.cpp
  src/talker.cpp
)

set(HEADERS
  includes/common.h
  includes/worker.h
  includes/listener.h
  includes/talker.h
)

# Define executable
add_executable(${PROJECT_NAME} ${SOURCES} ${HEADERS})

# Define dependencies
target_link_libraries(${PROJECT_NAME} pthread)

# Define compiler flags
target_compile_options(${PROJECT_NAME} PRIVATE -Wall -Wextra)

# Define debug flags
set(CMAKE_BUILD_TYPE Debug)
target_compile_options(${PROJECT_NAME} PRIVATE -DDEBUG)

# Define install rules
install(TARGETS ${PROJECT_NAME} DESTINATION bin)
install(FILES ${HEADERS} DESTINATION include)