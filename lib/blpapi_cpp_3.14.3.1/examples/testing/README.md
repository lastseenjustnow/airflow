# Blpapi testing support

`TestUtil` provides users with ability to test their applications offline
through the creation of custom events for their applications.

We are providing in this directory a simple application `MarketDataNotifier`
complete with tests.

Together with the example application we are providing code `snippets` that can
be used in your own tests and demonstrate the construction of some common
messages and events.

We have chosen to use `cmake` as the build system and `googletest` for our
tests, but the code can easily be ported to a different build system or
test framework.
