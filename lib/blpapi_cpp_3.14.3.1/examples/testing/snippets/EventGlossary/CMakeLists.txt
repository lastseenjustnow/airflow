add_executable(eventTests events.t.cpp)
target_link_libraries(eventTests PUBLIC blpapi gtest gmock)

gtest_add_tests(TARGET eventTests)
