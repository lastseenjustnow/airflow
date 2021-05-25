set(_SOURCES
    "appconfig.cpp"
    "application.cpp"
    "authorizer.cpp"
    "computeengine.cpp"
    "eventprocessor.cpp"
    "notifier.cpp"
    "subscriber.cpp"
    "tokengenerator.cpp")

add_library(marketDataNotifiersObjects OBJECT "${_SOURCES}")
target_include_directories(marketDataNotifiersObjects
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")

target_link_libraries(marketDataNotifiersObjects PUBLIC blpapi)

add_executable(marketDataNotifier main.cpp)
target_link_libraries(marketDataNotifier PUBLIC marketDataNotifiersObjects)
