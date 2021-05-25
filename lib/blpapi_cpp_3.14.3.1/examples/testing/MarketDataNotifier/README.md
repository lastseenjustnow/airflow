# MarketDataNotifier

This example includes an application and the unit tests for the application.

The MarketDataNotifier is an example for how a BLPAPI application can be
structured and unit tested.

The application source code is in `src/` with unit tests in `tests/`.

## Building and running

The application and associated test drivers can be built and run following
these steps:

1. `mkdir build`
2. `cd build`
3. `cmake -DCMAKE_PREFIX_PATH=<path to directory containing blpapiConfig.cmake file> ..`

If you don't have access to internet you can provide the path to googletest
source directory with the following command:

   `cmake -DCMAKE_PREFIX_PATH=<...> -DGTEST_SRC_DIR=<path to directory containing googletest src> ..`

4. `cmake --build . --config Release`. By default test cases are built in `Release` configuration.
5. `ctest` for platform other than Windows. For Windows use `ctest -C Release`

Configuration can be changed by passing `-DCMAKE_BUILD_TYPE=<desired-config>`
in step 3., and `--config <desired-config>` in step 4. Same configuration needs
to be passed to `ctest` in step 5.

## Description of the example

The example provided will test the application by mocking the essential objects.

The interfaces in the diagram give us the ability to create mock objects by
implementing these interfaces.

![](./diagram.png)

The Authorizer is responsible for setting up the authentication of the
application, this includes creating and sending the auth request and awaiting a
successful reply.

The Subscriber is responsible for setting up the subscription for certain topics.

The EventProcessor handles all the incoming events and triggers business logic
(ComputeEngine or Notifier).

The Notifier fires notifications within the system such as sending alerts to
the terminal.

The ComputeEngine does complex computations on incoming data and passes it off
to the Notifier.

The actual application does the following:

 * Sets up the necessary objects (Notifier, ComputeEngine, Session,
   TokenGenerator, Authorizer, Subscriber)
 * Starts the session
 * Authorizes the application
 * Subscribes to topics
 * Allows the EventProcessor to process the incoming events
 * Calling either methods from Notifier or ComputeEngine
