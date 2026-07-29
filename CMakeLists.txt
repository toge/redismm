cmake_minimum_required(VERSION 3.25)
project(redismm VERSION 0.1.0 LANGUAGES CXX)

option(BUILD_TEST "Enable tests" ON)

## add stdc++ compiler flag
set(STD_CPP cxx_std_23)

## add warning
if (CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g3")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
endif()

if (MSVC)
  if(CMAKE_CXX_FLAGS MATCHES "/W[0-4]")
    string(REGEX REPLACE "/W[0-4]" "/W4" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W4")
  endif()
endif()

## optimization
if (CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -march=native")
endif()
if (MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Ox")
endif()

## add LTO flag
if(NOT MSVC)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ipo_supported OUTPUT error)
  if(ipo_supported)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
  endif()
endif()

## other flag
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

## google perf tools
# HEAPPROFILE=heap CPUPROFILE=test.prof xxx
# pprof --text xxx yyy.prof
#set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lprofiler -ltcmalloc")

find_package(RocksDB CONFIG REQUIRED)

add_library(redismm src/EmbeddedRedis.cpp)
target_include_directories(redismm PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(redismm PUBLIC RocksDB::rocksdb)
target_compile_features(redismm PUBLIC ${STD_CPP})

# ---- Install / Export ----
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Install library target and export for find_package
install(TARGETS redismm
  EXPORT redismmTargets
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Install headers
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/redismm
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Install export file for downstream consumers
install(EXPORT redismmTargets
  FILE redismmTargets.cmake
  NAMESPACE redismm::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/redismm
)

set(INSTALL_CMAKE_DIR ${CMAKE_INSTALL_LIBDIR}/cmake/redismm)

configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/RedismmConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/RedismmConfig.cmake
  INSTALL_DESTINATION ${INSTALL_CMAKE_DIR}
)

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/RedismmConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY AnyNewerVersion
)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/RedismmConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/RedismmConfigVersion.cmake
  DESTINATION ${INSTALL_CMAKE_DIR}
)

# Export to the build tree as well
export(EXPORT redismmTargets
  FILE ${CMAKE_CURRENT_BINARY_DIR}/redismmTargets.cmake
  NAMESPACE redismm::
)

if(BUILD_TEST)
  add_executable(redismm_demo main.cpp)
  target_link_libraries(redismm_demo PRIVATE redismm)
  target_compile_features(redismm_demo PRIVATE ${STD_CPP})

  enable_testing()
  add_subdirectory(test)
endif()
