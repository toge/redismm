find_package(Catch2 REQUIRED CONFIG)

file(GLOB test_src test_*.cpp)

add_executable(all_test ${test_src})

target_link_libraries(all_test PRIVATE Catch2::Catch2 Catch2::Catch2WithMain redismm)
target_include_directories(all_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/include)
target_compile_features(all_test PRIVATE ${STD_CPP})
target_compile_features(all_test PRIVATE ${STD_CPP})

# ponytail: MSVC CI 0xc0000409 診断用に 3 段階で実行。原因判明後に元の 1 行に戻す。
add_test(NAME list_tests COMMAND all_test --list-tests)
add_test(NAME console_test COMMAND all_test)
add_test(NAME all_test COMMAND all_test -r junit)
