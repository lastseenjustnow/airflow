add_executable(refDataTests refdata.t.cpp)
target_include_directories(refDataTests PUBLIC blpapi)
target_link_libraries(refDataTests PUBLIC blpapi gtest gmock)

gtest_add_tests(TARGET refDataTests)
