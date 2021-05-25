/* Copyright 2019. Bloomberg Finance L.P.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:  The above
 * copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#include <blpapi_testutil.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace testing;

namespace blp = BloombergLP::blpapi;
namespace blptst = blp::test;

// Following test cases provides an example on how to mock different events
// supported by blpapi-sdk. The code to set up expectation and verification of
// values is ommitted from examples tests.

// SESSION STATUS EVENTS
// =====================
//
// Session status events provide notifications about the session and
// connectivity state.  The following tests provide examples of each one of the
// event/messages that the application may expect.
//
// Applications need to handle:
// - 'SessionStarted',
// - 'SessionStartupFailure'
// -  'SessionTerminated'
// 
// Applications can safely ignore:
// - 'SessionConnectionUp',
// - 'SessionConnectionDown'
// - 'SessionClusterInfo'
// - 'SessionClusterUpdate'
// 
// These events are informational only and applications should not react to
// them.

TEST(ExampleEvents, SessionStarted)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SESSION_STATUS);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SessionStarted"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                              "
                             "   \"initialEndpoints\": [                     "
                             "       { \"address\": \"12.34.56.78:8194\" },  "
                             "       { \"address\": \"98.76.54.32:8194\" }   "
                             "   ]                                           "
                             "}                                              ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, SessionStartupFailure)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SESSION_STATUS);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SessionStartupFailure"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                            "
                             "   \"reason\": {                             "
                             "       \"source\":      \"TestUtil\",        "
                             "       \"errorCode\":   -1,                  "
                             "       \"category\":    \"CATEGORY\",        "
                             "       \"description\": \"for testing\",     "
                             "       \"subcategory\": \"SUBCATEGORY\"      "
                             "    }                                        "
                             "}                                            ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, SessionTerminated)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SESSION_STATUS);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SessionTerminated"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                            "
                             "   \"reason\": {                             "
                             "       \"source\":      \"TestUtil\",        "
                             "       \"errorCode\":   -1,                  "
                             "       \"category\":    \"CATEGORY\",        "
                             "       \"description\": \"for testing\",     "
                             "       \"subcategory\": \"SUBCATEGORY\"      "
                             "    }                                        "
                             "}                                            ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, SessionConnectionUp)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SESSION_STATUS);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SessionConnectionUp"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                              "
                             "   \"server\":           \"12.34.56.78:8194\", "
                             "   \"serverId\":         \"ny-hostname\",      "
                             "   \"encryptionStatus\": \"Clear\"             "
                             "}                                              ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, SessionConnectionDown)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SESSION_STATUS);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SessionConnectionDown"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                              "
                             "   \"server\":           \"12.34.56.78:8194\", "
                             "   \"serverId\":         \"ny-hostname\"       "
                             "}                                              ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, SessionClusterInfo)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SESSION_STATUS);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SessionClusterInfo"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                              "
                             "   \"name\":           \"clustername\",        "
                             "   \"endpoints\": [                            "
                             "       { \"address\": \"12.34.56.78:8194\" },  "
                             "       { \"address\": \"98.76.54.32:8194\" }   "
                             "   ]                                           "
                             "}                                              ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, SessionClusterUpdate)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SESSION_STATUS);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SessionClusterUpdate"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                              "
                             "   \"name\":           \"clustername\",        "
                             "   \"endpointsAdded\": [                       "
                             "       { \"address\": \"12.34.56.78:8194\" }   "
                             "   ],                                          "
                             "   \"endpointsRemoved\": [                     "
                             "       { \"address\": \"98.76.54.32:8194\" }   "
                             "   ]                                           "
                             "}                                              ";
    fmtter.formatMessageJson(msgContent);
}

                                // ============
                                // ADMIN EVENTS
                                // ============

TEST(ExampleEvents, SlowConsumerWarning)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::ADMIN);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SlowConsumerWarning"));
    blptst::TestUtil::appendMessage(expectedEvent, schema);
}

TEST(ExampleEvents, SlowConsumerWarningCleared)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::ADMIN);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SlowConsumerWarningCleared"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                              "
                             "   \"eventsDropped\": 123                      "
                             "}                                              ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, DataLoss)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::ADMIN);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(blp::Name("DataLoss"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                              "
                             "   \"id\":                 \"id\",             "
                             "   \"source\":             \"Test\",           "
                             "   \"numMessagesDropped\": 12345               "
                             "}                                              ";

    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, RequestTemplateAvailable)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::ADMIN);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("RequestTemplateAvailable"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                                "
        "    \"boundTo\":  {                              "
        "        \"dataConnection\": [                    "
        "            {                                    "
        "               \"address\": \"12.34.56.78:8194\" "
        "            }                                    "
        "        ]                                        "
        "    }                                            "
        "}                                                ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, RequestTemplatePending)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::ADMIN);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("RequestTemplatePending"));

    blptst::TestUtil::appendMessage(expectedEvent, schema);
}

TEST(ExampleEvents, RequestTemplateTerminated)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::ADMIN);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("RequestTemplateTerminated"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                                 "
        "    \"reason\": {                                 "
        "        \"source\":      \"TestUtil\",            "
        "        \"errorCode\":   -1,                      "
        "        \"category\":    \"CATEGORY\",            "
        "        \"description\": \"for testing\",         "
        "        \"subcategory\": \"SUBCATEGORY\"          "
        "    }                                             "
        "}                                                 ";
    fmtter.formatMessageJson(msgContent);
}

                           // =====================
                           // SERVICE STATUS EVENTS
                           // =====================

TEST(ExampleEvents, ServiceOpened)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SERVICE_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("ServiceOpened"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                              "
                             "   \"serviceName\":  \"//blp/myservice\"       "
                             "}                                              ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, ServiceOpenFailure)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SERVICE_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("ServiceOpenFailure"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                            "
                             "   \"reason\": {                             "
                             "       \"source\":      \"TestUtil\",        "
                             "       \"errorCode\":   -1,                  "
                             "       \"category\":    \"CATEGORY\",        "
                             "       \"description\": \"for testing\",     "
                             "       \"subcategory\": \"SUBCATEGORY\"      "
                             "    }                                        "
                             "}                                            ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, ServiceRegistered)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SERVICE_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("ServiceRegistered"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                              "
                             "   \"serviceName\":  \"//blp/myservice\"       "
                             "}                                              ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, ServiceRegisterFailure)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SERVICE_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("ServiceRegisterFailure"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                            "
                             "   \"reason\": {                             "
                             "       \"source\":      \"TestUtil\",        "
                             "       \"errorCode\":   -1,                  "
                             "       \"category\":    \"CATEGORY\",        "
                             "       \"description\": \"for testing\",     "
                             "       \"subcategory\": \"SUBCATEGORY\"      "
                             "    }                                        "
                             "}                                            ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, ServiceDeregistered)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SERVICE_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("ServiceDeregistered"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                              "
                             "   \"serviceName\":  \"//blp/myservice\"       "
                             "}                                              ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, ServiceDown)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SERVICE_STATUS);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(blp::Name("ServiceDown"));

    // const char *msgContent =
    //     "{                                              "
    //     "   \"serviceName\":  \"//blp/myservice\",      "
    //     "   \"servicePart\":  {                         "
    //     "       \"publishing\": { }                     "
    //     "   },                                          "
    //     "   \"endpoint\": \"12.34.56.78\"               "
    //    "}                                              ";

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    // Existence of void types (e.g. "servicePart" elements") are not
    // well-supported by the JSON/XML interface. Instead `EventFormatter` type
    // interface, provided by `blptst::MessageFormatter` will be used for this.
    fmtter.setElement(blp::Name("serviceName"), "//blp/myservice");
    fmtter.pushElement(blp::Name("servicePart"));
    fmtter.setElementNull(blp::Name("publishing"));
    fmtter.popElement();
    fmtter.setElement(blp::Name("endpoint"), "12.34.56.78");
}

TEST(ExampleEvents, ServiceUp)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SERVICE_STATUS);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(blp::Name("ServiceUp"));

    // const char *msgContent =
    //     "{                                              "
    //     "   \"serviceName\":  \"//blp/myservice\",      "
    //     "   \"servicePart\":  {                         "
    //     "       \"publishing\": { }                     "
    //     "   },                                          "
    //     "   \"endpoint\": \"12.34.56.78\"               "
    //    "}                                              ";

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    // Existence of void types (e.g. "servicePart" elements") are not
    // well-supported by the JSON/XML interface. Instead `EventFormatter` type
    // interface, provided by `blptst::MessageFormatter` will be used for this.
    fmtter.setElement(blp::Name("serviceName"), "//blp/myservice");
    fmtter.pushElement(blp::Name("servicePart"));
    fmtter.setElementNull(blp::Name("publishing"));
    fmtter.popElement();
    fmtter.setElement(blp::Name("endpoint"), "12.34.56.78");
}

TEST(ExampleEvents, ServiceAvailabilityInfo)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SERVICE_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("ServiceAvailabilityInfo"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                              "
                             "   \"serviceName\":  \"//blp/myservice\",      "
                             "   \"serverAdded\":  {                         "
                             "       \"address\": \"12.34.56.78\"            "
                             "   },                                          "
                             "   \"serverRemoved\":  {                       "
                             "       \"address\": \"12.34.56.78\"            "
                             "   },                                          "
                             "   \"servers\": [                              "
                             "       \"12.34.56.78\",                        "
                             "       \"12.34.56.78\"                         "
                             "   ]                                           "
                             "}                                              ";
    fmtter.formatMessageJson(msgContent);
}

                            // ===================
                            // TOKEN STATUS EVENTS
                            // ===================

TEST(ExampleEvents, TokenGenerationSuccess)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::TOKEN_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("TokenGenerationSuccess"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                          "
                             "   \"token\": \"mytoken\"  "
                             "}                          ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, TokenGenerationFailure)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::TOKEN_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("TokenGenerationFailure"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                            "
                             "   \"reason\": {                             "
                             "       \"source\":      \"TestUtil\",        "
                             "       \"errorCode\":   -1,                  "
                             "       \"category\":    \"CATEGORY\",        "
                             "       \"description\": \"for testing\",     "
                             "       \"subcategory\": \"SUBCATEGORY\"      "
                             "    }                                        "
                             "}                                            ";
    fmtter.formatMessageJson(msgContent);
}

                         // ==========================
                         // SUBSCRIPTION STATUS EVENTS
                         // ==========================

TEST(ExampleEvents, SubscriptionStarted)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SUBSCRIPTION_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SubscriptionStarted"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                                 "
        "    \"exceptions\": [                             "
        "        {                                         "
        "            \"fieldId\": \"field\",               "
        "            \"reason\": {                         "
        "                \"source\":      \"TestUtil\",    "
        "                \"errorCode\":   -1,              "
        "                \"category\":    \"CATEGORY\",    "
        "                \"description\": \"for testing\", "
        "                \"subcategory\": \"SUBCATEGORY\"  "
        "            }                                     "
        "        }                                         "
        "    ],                                            "
        "    \"resubscriptionId\": 123,                    "
        "    \"streamIds\":        [                       "
        "        \"123\",                                  "
        "        \"456\"                                   "
        "    ],                                            "
        "    \"receivedFrom\": {                           "
        "         \"address\": \"12.34.56.78:8194\" }      "
        "    },                                            "
        "    \"reason\":      \"TestUtil\"                 "
        "}                                                 ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, SubscriptionFailure)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SUBSCRIPTION_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SubscriptionFailure"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                                 "
        "    \"reason\": {                                 "
        "        \"source\":      \"TestUtil\",            "
        "        \"errorCode\":   -1,                      "
        "        \"category\":    \"CATEGORY\",            "
        "        \"description\": \"for testing\",         "
        "        \"subcategory\": \"SUBCATEGORY\"          "
        "    },                                            "
        "    \"failureDetails\": [                         "
        "        {                                         "
        "            \"fieldId\": \"field\",               "
        "            \"reason\": {                         "
        "                \"source\":      \"TestUtil\",    "
        "                \"errorCode\":   -1,              "
        "                \"category\":    \"CATEGORY\",    "
        "                \"description\": \"for testing\", "
        "                \"subcategory\": \"SUBCATEGORY\"  "
        "            }                                     "
        "        }                                         "
        "    ],                                            "
        "    \"resubscriptionId\": 123                     "
        "}                                                 ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, SubscriptionStreamsActivated)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SUBSCRIPTION_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SubscriptionStreamsActivated"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                                 "
        "    \"streams\": [                                "
        "        {                                         "
        "            \"id\": \"streamId\",                 "
        "            \"endpoint\": {                       "
        "                \"address\": \"12.34.56.78:8194\" "
        "            }                                     "
        "        }                                         "
        "    ],                                            "
        "    \"reason\":  \"TestUtil\"                     "
        "}                                                 ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, SubscriptionStreamsDeactivated)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SUBSCRIPTION_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SubscriptionStreamsDeactivated"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                                 "
        "    \"streams\": [                                "
        "        {                                         "
        "            \"id\": \"streamId\",                 "
        "            \"endpoint\": {                       "
        "                \"address\": \"12.34.56.78:8194\" "
        "            }                                     "
        "        }                                         "
        "    ],                                            "
        "    \"reason\":  \"TestUtil\"                     "
        "}                                                 ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, SubscriptionTerminated)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::SUBSCRIPTION_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("SubscriptionTerminated"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                                 "
        "    \"reason\": {                                 "
        "        \"source\":      \"TestUtil\",            "
        "        \"errorCode\":   -1,                      "
        "        \"category\":    \"CATEGORY\",            "
        "        \"description\": \"for testing\",         "
        "        \"subcategory\": \"SUBCATEGORY\"          "
        "    }                                             "
        "}                                                 ";
    fmtter.formatMessageJson(msgContent);
}

                           // =====================
                           // REQUEST STATUS EVENTS
                           // =====================

TEST(ExampleEvents, RequestFailure)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::REQUEST_STATUS);

    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("RequestFailure"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                                 "
        "    \"reason\": {                                 "
        "        \"source\":      \"TestUtil\",            "
        "        \"errorCode\":   -1,                      "
        "        \"category\":    \"CATEGORY\",            "
        "        \"description\": \"for testing\",         "
        "        \"subcategory\": \"SUBCATEGORY\"          "
        "    }                                             "
        "}                                                 ";
    fmtter.formatMessageJson(msgContent);
}

                          // ========================
                          // RESOLUTION STATUS EVENTS
                          // ========================

TEST(ExampleEvents, ResolutionSuccess)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::RESOLUTION_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("ResolutionSuccess"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                                  "
        "   \"resolvedTopic\":  \"//blp/myservice/rtopic\"  "
        "}                                                  ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, ResolutionFailure)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::RESOLUTION_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("ResolutionFailure"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                                "
        "    \"reason\": {                                "
        "        \"source\":      \"TestUtil\",           "
        "        \"errorCode\":   -1,                     "
        "        \"category\":    \"CATEGORY\",           "
        "        \"description\": \"for testing\",        "
        "        \"subcategory\": \"SUBCATEGORY\"         "
        "    }                                            "
        "}                                                ";
    fmtter.formatMessageJson(msgContent);
}

                        // ============================
                        // PERMISSION STATUS OPERATIONS
                        // ============================

TEST(ExampleEvents, PermissionRequest)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::REQUEST);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("PermissionRequest"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                                 "
        "    \"topics\":        [ \"topic1\", \"topic2\"], "
        "    \"serviceName\":   \"//blp/mytestservice\",   "
        "    \"uuid\":          1234,                      "
        "    \"seatType\":      1234,                      "
        "    \"applicationId\": 1234,                      "
        "    \"userName\":      \"someName\",              "
        "    \"appName\":       \"myAppName\",             "
        "    \"deviceAddress\": \"myDevice\"               "
        "}                                                 ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, PermissionResponse)
{
    // Unlike the other admin messages, 'PermissionResponse' is not delivered
    // to applications by the SDK. It is used by resolvers to respond to
    // incoming 'PermissionRequest' messages. BLPAPI applications are not
    // expected to handle these messages.
    //
    // For testing if an application is publishing 'PermissionResponse'
    // messages with correct values, it is recommended to mock the related
    // `ProviderSession::publish()` method to capture the published events.
    // See the provided testing examples for more detail.
}

                            // ===================
                            // TOPIC STATUS EVENTS
                            // ===================

TEST(ExampleEvents, TopicCreated)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::TOPIC_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(blp::Name("TopicCreated"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                               "
        "    \"topic\":       \"mytopic\"                "
        "}                                               ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, TopicCreateFailure)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::TOPIC_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("TopicCreateFailure"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                         "
                             "    \"topic\":       \"mytopic\",         "
                             "    \"reason\": {                         "
                             "        \"source\":      \"TestUtil\",    "
                             "        \"errorCode\":   -1,              "
                             "        \"category\":    \"CATEGORY\",    "
                             "        \"description\": \"for testing\", "
                             "        \"subcategory\": \"SUBCATEGORY\"  "
                             "    }                                     "
                             "}                                         ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, TopicDeleted)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::TOPIC_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(blp::Name("TopicDeleted"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                             "
                             "    \"topic\":       \"mytopic\",             "
                             "    \"reason\":      \"TestUtil\"             "
                             "}                                             ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, TopicSubscribed)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::TOPIC_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("TopicSubscribed"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                             "
                             "    \"topic\":            \"mytopic\",        "
                             "    \"topicWithOptions\": \"topicwithopts\"   "
                             "}                                             ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, TopicResubscribed)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::TOPIC_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("TopicResubscribed"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                             "
                             "    \"topic\":            \"mytopic\",        "
                             "    \"topicWithOptions\": \"topicwithopts\",  "
                             "    \"reason\":           \"TestUtil\"        "
                             "}                                             ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, TopicUnsubscribed)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::TOPIC_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("TopicUnsubscribed"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                             "
                             "    \"topic\":            \"mytopic\",        "
                             "    \"reason\":           \"TestUtil\"        "
                             "}                                             ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, TopicActivated)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::TOPIC_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("TopicActivated"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent =
        "{                                               "
        "    \"topic\":       \"mytopic\"                "
        "}                                               ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, TopicDeactivated)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::TOPIC_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(
            blp::Name("TopicDeactivated"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                             "
                             "    \"topic\":       \"mytopic\",             "
                             "    \"reason\":      \"TestUtil\"             "
                             "}                                             ";
    fmtter.formatMessageJson(msgContent);
}

TEST(ExampleEvents, TopicRecap)
{
    blp::Event expectedEvent =
        blptst::TestUtil::createEvent(blp::Event::TOPIC_STATUS);
    const blp::SchemaElementDefinition schema =
        blptst::TestUtil::getAdminMessageDefinition(blp::Name("TopicRecap"));

    blptst::MessageFormatter fmtter =
        blptst::TestUtil::appendMessage(expectedEvent, schema);

    const char *msgContent = "{                                             "
                             "    \"topic\":       \"mytopic\",             "
                             "    \"isSolicited\": true,                    "
                             "    \"topicWithOptions\": \"topicwithopts\"   "
                             "}                                             ";
    fmtter.formatMessageJson(msgContent);
}

int main(int argc, char **argv)
{
    // The following line must be executed to initialize Google Mock (and
    // Google Test) before running the tests.
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
