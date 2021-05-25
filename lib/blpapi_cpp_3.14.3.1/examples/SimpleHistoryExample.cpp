/* Copyright 2012. Bloomberg Finance L.P.
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
#include <blpapi_session.h>
#include <blpapi_eventdispatcher.h>

#include <blpapi_event.h>
#include <blpapi_message.h>
#include <blpapi_element.h>
#include <blpapi_name.h>
#include <blpapi_request.h>
#include <blpapi_subscriptionlist.h>
#include <blpapi_defs.h>
#include <blpapi_exception.h>

#include <iostream>
#include <vector>
#include <string>
#include <stdlib.h>
#include <string.h>
#include <ctime>
#include <fstream>

using namespace BloombergLP;
using namespace blpapi;

namespace{

Name TOKEN_SUCCESS("TokenGenerationSuccess");
Name TOKEN_FAILURE("TokenGenerationFailure");
Name AUTHORIZATION_SUCCESS("AuthorizationSuccess");
Name TOKEN("token");


const std::string AUTH_USER       = "AuthenticationType=OS_LOGON";
const std::string AUTH_APP_PREFIX = "AuthenticationMode=APPLICATION_ONLY;"
                                    "ApplicationAuthenticationType=APPNAME_AND_KEY;"
                                    "ApplicationName=";
const std::string AUTH_USER_APP_PREFIX = "AuthenticationMode=USER_AND_APPLICATION;"
                                         "AuthenticationType=OS_LOGON;"
                                         "ApplicationAuthenticationType=APPNAME_AND_KEY;"
                                         "ApplicationName=";
const std::string AUTH_USER_APP_MANUAL_PREFIX = "AuthenticationMode=USER_AND_APPLICATION;"
                                                "AuthenticationType=MANUAL;"
                                                "ApplicationAuthenticationType=APPNAME_AND_KEY;"
                                                "ApplicationName=";
const std::string AUTH_DIR_PREFIX = "AuthenticationType=DIRECTORY_SERVICE;"
                                    "DirSvcPropertyName=";

const char* AUTH_OPTION_NONE      = "none";
const char* AUTH_OPTION_USER      = "user";
const char* AUTH_OPTION_APP       = "app=";
const char* AUTH_OPTION_USER_APP  = "userapp=";
const char* AUTH_OPTION_DIR       = "dir=";
const char* AUTH_OPTION_MANUAL    = "manual=";

std::vector<std::string> splitBy(const std::string& str, char delim)
{
    std::string::size_type start = 0u, pos = 0u;
    std::vector<std::string> tokens;
    while ((pos = str.find(delim, start)) != std::string::npos) {
        tokens.push_back(str.substr(start, pos - start));
        start = pos + 1;
    }
    if (start != str.size()) {
        tokens.push_back(str.substr(start));
    }
    return tokens;
}

int readFromFile(const std::string& filename, std::string& buffer)
{
    std::ifstream in(filename.c_str(),
                     std::ios::in | std::ios::binary | std::ios::ate);
    if (in) {
        buffer.resize(in.tellg());
        in.seekg(0, std::ios::beg);
        in.read(&buffer[0], buffer.size());
    }

    if (in.fail()) {
        std::cerr << "Failed to read file from " << filename << std::endl;
        return 1;
    }

    std::cout << "Read " << buffer.size() << " bytes from "
              << filename << std::endl;
    return 0;
}

}

class SimpleHistoryExample
{
    std::string         d_host;
    int                 d_port;
    std::string         d_authOptions;
    std::string         d_clientCredentials;
    std::string         d_clientCredentialsPassword;
    std::string         d_trustMaterial;
    bool                d_readTlsData;

    void printUsage()
    {
        std::cout << "Send historical data request." << std::endl
            << "Usage:" << std::endl
            << "\t[-ip   <ipAddress = localhost>" << std::endl
            << "\t[-p    <tcpPort   = 8194>" << std::endl
            << "\t[-auth <option>] \tauthentication option: user|none|app=<app>|userapp=<app>|dir=<property> (default: none)" << std::endl
            << std::endl
            << "TLS OPTIONS (specify all or none):\n"
               "\t[-tls-client-credentials <file>] \tname a PKCS#12 file to use as a source of client credentials\n"
               "\t[-tls-client-credentials-password <pwd>] \tspecify password for accessing client credentials\n"
               "\t[-tls-trust-material <file>]     \tname a PKCS#7 file to use as a source of trusted certificates\n"
               "\t[-read-certificate-files]        \t(optional) read the TLS files and pass the blobs\n"
            << std::endl;
    }

    bool parseCommandLine(int argc, char **argv)
    {
        for (int i = 1; i < argc; ++i) {
            if (!std::strcmp(argv[i],"-ip") && i + 1 < argc) {
                d_host = argv[++i];
                continue;
            } else if (!std::strcmp(argv[i],"-p") &&  i + 1 < argc) {
                d_port = std::atoi(argv[++i]);
                continue;
            } else if (!std::strcmp(argv[i], "-auth") && i + 1 < argc) {
                ++ i;
                if (!std::strcmp(argv[i], AUTH_OPTION_NONE)) {
                    d_authOptions.clear();
                }
                else if (strncmp(argv[i], AUTH_OPTION_APP, strlen(AUTH_OPTION_APP)) == 0) {
                    d_authOptions.clear();
                    d_authOptions.append(AUTH_APP_PREFIX);
                    d_authOptions.append(argv[i] + strlen(AUTH_OPTION_APP));
                }
                else if (strncmp(argv[i], AUTH_OPTION_USER_APP, strlen(AUTH_OPTION_USER_APP)) == 0) {
                    d_authOptions.clear();
                    d_authOptions.append(AUTH_USER_APP_PREFIX);
                    d_authOptions.append(argv[i] + strlen(AUTH_OPTION_USER_APP));
                }
                else if (strncmp(argv[i], AUTH_OPTION_DIR, strlen(AUTH_OPTION_DIR)) == 0) {
                    d_authOptions.clear();
                    d_authOptions.append(AUTH_DIR_PREFIX);
                    d_authOptions.append(argv[i] + strlen(AUTH_OPTION_DIR));
                }
                else if (!std::strcmp(argv[i], AUTH_OPTION_USER)) {
                    d_authOptions.assign(AUTH_USER);
                }
                else if (!std::strncmp(argv[i], AUTH_OPTION_MANUAL, strlen(AUTH_OPTION_MANUAL))) {
                    std::vector<std::string> elems = splitBy(argv[i] + strlen(AUTH_OPTION_MANUAL), ',');
                    if (elems.size() != 3u) {
                        std::cerr << "Invalid auth option: " << argv[i] << '\n';
                        printUsage();
                        return false;
                    }
                    d_authOptions.clear();
                    d_authOptions.append(AUTH_USER_APP_MANUAL_PREFIX);
                    d_authOptions.append(elems[0]);
                }
                else {
                    printUsage();
                    return false;
                }
            } else if (!std::strcmp(argv[i], "-tls-client-credentials") && i + 1 < argc) {
                d_clientCredentials = argv[++i];
            } else if (!std::strcmp(argv[i], "-tls-client-credentials-password") && i + 1 < argc) {
                d_clientCredentialsPassword = argv[++i];
            } else if (!std::strcmp(argv[i], "-tls-trust-material") && i + 1 < argc) {
                d_trustMaterial = argv[++i];
            } else if (!std::strcmp(argv[i], "-read-certificate-files")) {
                d_readTlsData = true;
            } else {
                printUsage();
                return false;
            }
        }
        return true;
    }

    bool authorize(const Service &authService,
                   Identity *subscriptionIdentity,
                   Session *session)
    {
        EventQueue tokenEventQueue;
        session->generateToken(CorrelationId(), &tokenEventQueue);
        std::string token;
        Event event = tokenEventQueue.nextEvent();
        if (event.eventType() == Event::TOKEN_STATUS ||
            event.eventType() == Event::REQUEST_STATUS) {
            MessageIterator iter(event);
            while (iter.next()) {
                Message msg = iter.message();
                msg.print(std::cout);
                if (msg.messageType() == TOKEN_SUCCESS) {
                    token = msg.getElementAsString(TOKEN);
                }
                else if (msg.messageType() == TOKEN_FAILURE) {
                    break;
                }
            }
        }
        if (token.length() == 0) {
            std::cout << "Failed to get token" << std::endl;
            return false;
        }

        Request authRequest = authService.createAuthorizationRequest();
        authRequest.set(TOKEN, token.c_str());

        session->sendAuthorizationRequest(authRequest, subscriptionIdentity);

        time_t startTime = time(0);
        const int WAIT_TIME_SECONDS = 10;
        while (true) {
            Event event = session->nextEvent(WAIT_TIME_SECONDS * 1000);
            if (event.eventType() == Event::RESPONSE ||
                event.eventType() == Event::REQUEST_STATUS ||
                event.eventType() == Event::PARTIAL_RESPONSE) {
                MessageIterator msgIter(event);
                while (msgIter.next()) {
                    Message msg = msgIter.message();
                    msg.print(std::cout);
                    if (msg.messageType() == AUTHORIZATION_SUCCESS) {
                        return true;
                    }
                    else {
                        std::cout << "Authorization failed" << std::endl;
                        return false;
                    }
                }
            }
            time_t endTime = time(0);
            if (endTime - startTime > WAIT_TIME_SECONDS) {
                return false;
            }
        }
    }

public:
    SimpleHistoryExample()
        : d_port(8196)
        , d_host("localhost")
        , d_authOptions("")
        , d_readTlsData(false)
    {
    }

    void run(int argc, char **argv)
    {
        if (!parseCommandLine(argc, argv)) return;

        SessionOptions sessionOptions;
        sessionOptions.setServerHost(d_host.c_str());
        sessionOptions.setServerPort(d_port);
        sessionOptions.setAuthenticationOptions(d_authOptions.c_str());

        std::cout << "Connecting to " <<  d_host << ":" << d_port << std::endl;

        if (d_clientCredentials.size()
                && d_trustMaterial.size()) {

            std::cout << "TlsOptions enabled" << std::endl;
            TlsOptions tlsOptions;
            if (d_readTlsData) {
                std::string clientCredentials;
                std::string trustMaterial;

                if (readFromFile(d_clientCredentials, clientCredentials)
                        || readFromFile(d_trustMaterial, trustMaterial)) {
                    // Failed to read from files
                    return;
                }

                tlsOptions = TlsOptions::createFromBlobs(
                                        clientCredentials.data(),
                                        clientCredentials.size(),
                                        d_clientCredentialsPassword.c_str(),
                                        trustMaterial.data(),
                                        trustMaterial.size());
            } else {
                tlsOptions =
                    TlsOptions::createFromFiles(
                                        d_clientCredentials.c_str(),
                                        d_clientCredentialsPassword.c_str(),
                                        d_trustMaterial.c_str());
            }
            sessionOptions.setTlsOptions(tlsOptions);
        }

        Session session(sessionOptions);
        if (!session.start()) {
            std::cerr <<"Failed to start session." << std::endl;
            return;
        }

        Identity identity;
        if (!d_authOptions.empty()) {
            identity = session.createIdentity();
            bool isAuthorized = false;
            const char* authServiceName = "//blp/apiauth";
            if (session.openService(authServiceName)) {
                Service authService = session.getService(authServiceName);
                isAuthorized = authorize(authService, &identity, &session);
            }
            if (!isAuthorized) {
                std::cerr << "No authorization" << std::endl;
                return;
            }
        }

        if (!session.openService("//blp/refdata")) {
            std::cerr <<"Failed to open //blp/refdata" << std::endl;
            return;
        }
        Service refDataService = session.getService("//blp/refdata");
        Request request = refDataService.createRequest("HistoricalDataRequest");
        request.getElement("securities").appendValue("IBM US Equity");
        request.getElement("securities").appendValue("MSFT US Equity");
        request.getElement("fields").appendValue("PX_LAST");
        request.getElement("fields").appendValue("OPEN");
        request.set("periodicityAdjustment", "ACTUAL");
        request.set("periodicitySelection", "MONTHLY");
        request.set("startDate", "20060101");
        request.set("endDate", "20061231");
        request.set("maxDataPoints", 100);

        std::cout << "Sending Request: " << request << std:: endl;
        session.sendRequest(request, identity);

        while (true) {
            Event event = session.nextEvent();
            MessageIterator msgIter(event);
            while (msgIter.next()) {
                Message msg = msgIter.message();
                msg.asElement().print(std::cout);
                std::cout << std::endl;
            }
            if (event.eventType() == Event::RESPONSE) {
                break;
            }
        }
    }
};

int main(int argc, char **argv)
{
    std::cout << "SimpleHistoryExample" << std::endl;
    SimpleHistoryExample example;
    try {
        example.run(argc, argv);
    } catch (Exception &e) {
        std::cerr << "Library Exception!!! " << e.description() << std::endl;
    }
    // wait for enter key to exit application
    std::cout << "Press ENTER to quit" << std::endl;
    char dummy[2];
    std::cin.getline(dummy, 2);
    return 0;
}
