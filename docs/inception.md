# Java logging library
## Introduction
The purpose of the java logging library is to format logging messages in such a way that is conformant to that which
is expected by the `md-logging-agent` service.

## Library properties
1. Presents an interface compatible with Log4J, such that a logger can be instantiated within a consuming codebase
   1. The format includes a fully compatible output of the expected message envelope of the `md-logging-agent` service
2. The consuming codebase would provide configuration necessary, and the library has an interface surface to receive the same, such that the library enables writing to an amqp queue to which the consuming service has access provisioned.
3. The library is accompanied by a full suite of unit tests, and build workflows, both for PRs and master branches, that include running said tests.
