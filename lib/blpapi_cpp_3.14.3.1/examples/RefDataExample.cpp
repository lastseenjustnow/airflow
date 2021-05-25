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
#include <blpapi_defs.h>
#include <blpapi_element.h>
#include <blpapi_event.h>
#include <blpapi_eventdispatcher.h>
#include <blpapi_exception.h>
#include <blpapi_logging.h>
#include <blpapi_message.h>
#include <blpapi_name.h>
#include <blpapi_request.h>
#include <blpapi_session.h>
#include <blpapi_subscriptionlist.h>
#include <blpapi_zfputil.h>

#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>
#include <ctime>

#include <stdlib.h>
#include <string.h>

using namespace BloombergLP;
using namespace blpapi;
namespace {
    const Name SECURITY_DATA("securityData");
    const Name SECURITY("security");
    const Name FIELD_DATA("fieldData");
    const Name RESPONSE_ERROR("responseError");
    const Name SECURITY_ERROR("securityError");
    const Name FIELD_EXCEPTIONS("fieldExceptions");
    const Name FIELD_ID("fieldId");
    const Name ERROR_INFO("errorInfo");
    const Name CATEGORY("category");
    const Name MESSAGE("message");
    const Name REASON("reason");
    const Name SESSION_TERMINATED("SessionTerminated");
    const Name SESSION_STARTUP_FAILURE("SessionStartupFailure");

    const Name TOKEN_SUCCESS("TokenGenerationSuccess");
    const Name TOKEN_FAILURE("TokenGenerationFailure");
    const Name AUTHORIZATION_SUCCESS("AuthorizationSuccess");
    const Name TOKEN("token");

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

};

extern "C" {
    void loggingCallback(blpapi_UInt64_t    threadId,
                         int                severity,
                         blpapi_Datetime_t  timestamp,
                         const char        *category,
                         const char        *message);
}

void loggingCallback(blpapi_UInt64_t    threadId,
                     int                severity,
                     blpapi_Datetime_t  timestamp,
                     const char        *category,
                     const char        *message)
{
    std::string severityString;
    switch(severity) {
    // The following cases will not happen if callback registered at OFF
    case blpapi_Logging_SEVERITY_FATAL:
    {
        severityString = "FATAL";
    } break;
    // The following cases will not happen if callback registered at FATAL
    case blpapi_Logging_SEVERITY_ERROR:
    {
        severityString = "ERROR";
    } break;
    // The following cases will not happen if callback registered at ERROR
    case blpapi_Logging_SEVERITY_WARN:
    {
        severityString = "WARN";
    } break;
    // The following cases will not happen if callback registered at WARN
    case blpapi_Logging_SEVERITY_INFO:
    {
        severityString = "INFO";
    } break;
    // The following cases will not happen if callback registered at INFO
    case blpapi_Logging_SEVERITY_DEBUG:
    {
        severityString = "DEBUG";
    } break;
    // The following case will not happen if callback registered at DEBUG
    case blpapi_Logging_SEVERITY_TRACE:
    {
        severityString = "TRACE";
    } break;

    };
    std::stringstream sstream;
    sstream << category <<" [" << severityString << "] Thread ID = "
            << threadId << ": " << message << std::endl;
    std::cout << sstream.str() << std::endl;;
}

class RefDataExample
{
    std::string              d_host;
    int                      d_port;
    std::vector<std::string> d_securities;
    std::vector<std::string> d_fields;
    std::string              d_authOptions;

    std::string              d_clientCredentials;
    std::string              d_clientCredentialsPassword;
    std::string              d_trustMaterial;
    bool                     d_readTlsData;

    bool                     d_zfpOverLeasedLine;
    ZfpUtil::Remote          d_remote;

    bool parseCommandLine(int argc, char **argv)
    {
        int verbosityCount = 0;
        for (int i = 1; i < argc; ++i) {
            if (!std::strcmp(argv[i],"-s") && i + 1 < argc) {
                d_securities.push_back(argv[++i]);
            } else if (!std::strcmp(argv[i],"-f") && i + 1 < argc) {
                d_fields.push_back(argv[++i]);
            } else if (!std::strcmp(argv[i],"-ip") && i + 1 < argc) {
                d_host = argv[++i];
            } else if (!std::strcmp(argv[i],"-p") &&  i + 1 < argc) {
                d_port = std::atoi(argv[++i]);
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
            } else if (!std::strcmp(argv[i],"-v")) {
                ++verbosityCount;
            }
            else if (!std::strcmp(argv[i], "-zfp-over-leased-line") && i + 1 < argc) {
                d_zfpOverLeasedLine = true;
                ++i;
                if (!std::strcmp(argv[i], "8194")) {
                    d_remote = ZfpUtil::REMOTE_8194;
                }
                else if (!std::strcmp(argv[i], "8196")) {
                    d_remote = ZfpUtil::REMOTE_8196;
                }
                else {
                    std::cerr << "Invalid ZFP port: " << argv[i] << "\n\n";
                    printUsage();
                    return false;
                }
            }
            else {
                printUsage();
                return false;
            }
        }
        if(verbosityCount) {
            registerCallback(verbosityCount);
        }
        // handle default arguments
        if (d_securities.size() == 0) {
            d_securities.push_back("IBM US Equity");
        }

        if (d_fields.size() == 0) {
            d_fields.push_back("PX_LAST");
        }

        return true;
    }

    void printErrorInfo(const char *leadingStr, const Element &errorInfo)
    {
        std::cout << leadingStr
            << errorInfo.getElementAsString(CATEGORY)
            << " ("
            << errorInfo.getElementAsString(MESSAGE)
            << ")" << std::endl;
    }

    void printUsage()
    {
        std::cout << "Usage:" << std::endl
            << "    Retrieve reference data " << std::endl
            << "        [-s         <security   = IBM US Equity>" << std::endl
            << "        [-f         <field      = PX_LAST>" << std::endl
            << "        [-ip        <ipAddress  = localhost>" << std::endl
            << "        [-p         <tcpPort    = 8194>" << std::endl
            << "\t[-auth <option>] \tauthentication option: user|none|app=<app>|userapp=<app>|dir=<property> (default: none)" << std::endl
            << std::endl
            << "TLS OPTIONS (specify all or none):\n"
               "\t[-tls-client-credentials <file>] \tname a PKCS#12 file to use as a source of client credentials\n"
               "\t[-tls-client-credentials-password <pwd>] \tspecify password for accessing client credentials\n"
               "\t[-tls-trust-material <file>]     \tname a PKCS#7 file to use as a source of trusted certificates\n"
               "\t[-read-certificate-files]        \t(optional) read the TLS files and pass the blobs\n"
            << "        [-v         increase verbosity"
            <<                    " (can be specified more than once)"
            << std::endl
            << "ZFP connections over leased lines (requires TLS options):\n"
            << "\t[-zfp-over-leased-line <port>]       enable ZFP connections over leased lines on the specified port (8194 or 8196)\n"
            << "\t\tWhen this option is enabled, '-ip' and '-p' arguments will be ignored.\n\n";
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

    void registerCallback(int verbosityCount)
    {
        blpapi_Logging_Severity_t severity = blpapi_Logging_SEVERITY_OFF;
        switch(verbosityCount) {
          case 1: {
              severity = blpapi_Logging_SEVERITY_INFO;
          }break;
          case 2: {
              severity = blpapi_Logging_SEVERITY_DEBUG;
          }break;
          default: {
              severity = blpapi_Logging_SEVERITY_TRACE;
          }
        };
        blpapi_Logging_registerCallback(loggingCallback,
                                        severity);
    }

    void sendRefDataRequest(Session &session, const Identity& identity)
    {
        Service refDataService = session.getService("//blp/refdata");
        Request request = refDataService.createRequest("ReferenceDataRequest");

        // Add securities to request
        Element securities = request.getElement("securities");
        for (size_t i = 0; i < d_securities.size(); ++i) {
            securities.appendValue(d_securities[i].c_str());
        }

        // Add fields to request
        Element fields = request.getElement("fields");
        for (size_t i = 0; i < d_fields.size(); ++i) {
            fields.appendValue(d_fields[i].c_str());
        }

        std::cout << "Sending Request: " << request << std::endl;
        session.sendRequest(request, identity);
    }

    // return true if processing is completed, false otherwise
    void processResponseEvent(Event event)
    {
        MessageIterator msgIter(event);
        while (msgIter.next()) {
            Message msg = msgIter.message();
            if (msg.asElement().hasElement(RESPONSE_ERROR)) {
                printErrorInfo("REQUEST FAILED: ",
                    msg.getElement(RESPONSE_ERROR));
                continue;
            }

            Element securities = msg.getElement(SECURITY_DATA);
            size_t numSecurities = securities.numValues();
            std::cout << "Processing " << (unsigned int)numSecurities
                      << " securities:"<< std::endl;
            for (size_t i = 0; i < numSecurities; ++i) {
                Element security = securities.getValueAsElement(i);
                std::string ticker = security.getElementAsString(SECURITY);
                std::cout << "\nTicker: " + ticker << std::endl;
                if (security.hasElement(SECURITY_ERROR)) {
                    printErrorInfo("\tSECURITY FAILED: ",
                        security.getElement(SECURITY_ERROR));
                    continue;
                }

                if (security.hasElement(FIELD_DATA)) {
                    const Element fields = security.getElement(FIELD_DATA);
                    if (fields.numElements() > 0) {
                        std::cout << "FIELD\t\tVALUE"<<std::endl;
                        std::cout << "-----\t\t-----"<< std::endl;
                        size_t numElements = fields.numElements();
                        for (size_t j = 0; j < numElements; ++j) {
                            Element field = fields.getElement(j);
                            std::cout << field.name() << "\t\t" <<
                                field.getValueAsString() << std::endl;
                        }
                    }
                }
                std::cout << std::endl;
                Element fieldExceptions = security.getElement(
                                                             FIELD_EXCEPTIONS);
                if (fieldExceptions.numValues() > 0) {
                    std::cout << "FIELD\t\tEXCEPTION" << std::endl;
                    std::cout << "-----\t\t---------" << std::endl;
                    for (size_t k = 0; k < fieldExceptions.numValues(); ++k) {
                        Element fieldException =
                            fieldExceptions.getValueAsElement(k);
                        Element errInfo = fieldException.getElement(
                                                                   ERROR_INFO);
                        std::cout
                                 << fieldException.getElementAsString(FIELD_ID)
                                 << "\t\t"
                                 << errInfo.getElementAsString(CATEGORY)
                                 << " ( "
                                 << errInfo.getElementAsString(MESSAGE)
                                 << ")"
                                 << std::endl;
                    }
                }
            }
        }
    }

    void eventLoop(Session &session)
    {
        bool done = false;
        while (!done) {
            Event event = session.nextEvent();
            if (event.eventType() == Event::PARTIAL_RESPONSE) {
                std::cout << "Processing Partial Response" << std::endl;
                processResponseEvent(event);
            }
            else if (event.eventType() == Event::RESPONSE) {
                std::cout << "Processing Response" << std::endl;
                processResponseEvent(event);
                done = true;
            } else {
                MessageIterator msgIter(event);
                while (msgIter.next()) {
                    Message msg = msgIter.message();
                    if (event.eventType() == Event::REQUEST_STATUS) {
                        std::cout << "REQUEST FAILED: " << msg.getElement(REASON) << std::endl;
                        done = true;
                    }
                    else if (event.eventType() == Event::SESSION_STATUS) {
                        if (msg.messageType() == SESSION_TERMINATED ||
                            msg.messageType() == SESSION_STARTUP_FAILURE) {
                            done = true;
                        }
                    }
                }
            }
        }
    }

    bool prepareTlsOptions(TlsOptions& tlsOptions)
    {
        if (d_readTlsData) {
            std::string clientCredentials;
            std::string trustMaterial;

            if (readFromFile(d_clientCredentials, clientCredentials)
                || readFromFile(d_trustMaterial, trustMaterial)) {
                // Failed to read from files
                return false;
            }

            tlsOptions = TlsOptions::createFromBlobs(
                clientCredentials.data(),
                clientCredentials.size(),
                d_clientCredentialsPassword.c_str(),
                trustMaterial.data(),
                trustMaterial.size());
        }
        else {
            tlsOptions =
                TlsOptions::createFromFiles(
                d_clientCredentials.c_str(),
                d_clientCredentialsPassword.c_str(),
                d_trustMaterial.c_str());
        }
        return true;
    }

    bool prepareStandardSessionOptions(SessionOptions& sessionOptions)
    {
        sessionOptions.setServerHost(d_host.c_str());
        sessionOptions.setServerPort(d_port);
        sessionOptions.setAuthenticationOptions(d_authOptions.c_str());
        sessionOptions.setAutoRestartOnDisconnection(true);

        if (d_clientCredentials.size()
            && d_trustMaterial.size()) {

            std::cout << "TlsOptions enabled" << std::endl;
            TlsOptions tlsOptions;

            if (!prepareTlsOptions(tlsOptions)) {
                return false;
            }

            sessionOptions.setTlsOptions(tlsOptions);
        }
        return true;
    }

    bool prepareZfpSessionOptions(SessionOptions& sessionOptions)
    {
        if (d_clientCredentials.empty() || d_trustMaterial.empty()) {
            std::cerr << "ZFP connections require TLS parameters";
            return false;
        }

        std::cout << "Creating a ZFP connection for leased lines." << std::endl;
        TlsOptions tlsOptions;
        if (!prepareTlsOptions(tlsOptions)) {
            return false;
        }
        sessionOptions = ZfpUtil::getZfpOptionsForLeasedLines(
            d_remote, tlsOptions);
        sessionOptions.setAuthenticationOptions(d_authOptions.c_str());
        return true;
    }

    bool prepareSessionOptions(SessionOptions& sessionOptions)
    {
        if (d_zfpOverLeasedLine) {
            if (!prepareZfpSessionOptions(sessionOptions)) {
                return false;
            }
        }
        else {
            if (!prepareStandardSessionOptions(sessionOptions)) {
                return false;
            }
        }
        return true;
    }

public:
    RefDataExample()
    {
        d_host = "localhost";
        d_port = 8194;
        d_authOptions = "";
        d_readTlsData = false;
        d_zfpOverLeasedLine = false;
    }

    ~RefDataExample()
    {
    }

    void run(int argc, char **argv)
    {
        if (!parseCommandLine(argc, argv)) return;

        SessionOptions sessionOptions;
        if (!prepareSessionOptions(sessionOptions)) {
            return;
        }

        Session session(sessionOptions);
        if (!session.start()) {
            std::cout << "Failed to start session." << std::endl;
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
            std::cout << "Failed to open //blp/refdata" << std::endl;
            return;
        }
        sendRefDataRequest(session, identity);

        // wait for events from session.
        try {
            eventLoop(session);
        } catch (Exception &e) {
            std::cerr << "Library Exception !!!"
                      << e.description()
                      << std::endl;
        } catch (...) {
            std::cerr << "Unknown Exception !!!" << std::endl;
        }


        session.stop();
    }
};

int main(int argc, char **argv)
{
    std::cout << "RefDataExample" << std::endl;
    RefDataExample example;
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
