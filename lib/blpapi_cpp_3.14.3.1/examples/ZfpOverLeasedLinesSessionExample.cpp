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

#include <blpapi_event.h>
#include <blpapi_exception.h>
#include <blpapi_message.h>
#include <blpapi_session.h>
#include <blpapi_tlsoptions.h>
#include <blpapi_zfputil.h>

#include <iostream>
#include <string>

using namespace BloombergLP;
using namespace blpapi;

namespace {

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

}

// The goal of this example is to demonstrate how to establish a ZFP session
// that leverages private leased line connectivity. To see how to use the
// resulting session (authorizing a session, establishing subscriptions or
// making requests etc.), please refer to the other examples.
class ZfpOverLeasedLinesSessionExample
{
    ZfpUtil::Remote d_remote;

    std::string d_clientCredentials;
    std::string d_clientCredentialsPassword;
    std::string d_trustMaterial;

    std::string d_authOptions;
    bool        d_manualToken;
    std::string d_manualUserId;
    std::string d_manualIPAddress;

    void printUsage()
    {
        std::cout <<
"ZFP over leased lines session startup\n"
"Usage:\n"
"\t[-zfp-over-leased-line <port>   enable ZFP connections over leased lines on the specified port (8194 or 8196) (default: 8194)\n"
"\t[-auth <option>]                authentication option (default: user):\n"
"\t\tnone\n"
"\t\tuser                     as a user using OS logon information\n"
"\t\tdir=<property>           as a user using directory services\n"
"\t\tapp=<app>                as the specified application\n"
"\t\tuserapp=<app>            as user and application using logon information\n"
"\t\t                         for the user\n"
"\t\tmanual=<app>,<ip>,<user> as user and application, with manually provided\n"
"\t\t                         IP address and EMRS user\n"
"\n"
"TLS OPTIONS (specify all):\n"
"\t[-tls-client-credentials <file>]         name a PKCS#12 file to use as a source of client credentials\n"
"\t[-tls-client-credentials-password <pwd>] specify password for accessing client credentials\n"
"\t[-tls-trust-material <file>]             name a PKCS#7 file to use as a source of trusted certificates\n"
<< std::endl;
    }

    bool parseCommandLine(int argc, char **argv)
    {
        for (int i = 1; i < argc; ++i) {
            if (!std::strcmp(argv[i],"-zfp-over-leased-line") && i + 1 < argc) {
                ++i;
                if (std::strcmp(argv[i], "8194") == 0) {
                    d_remote = ZfpUtil::REMOTE_8194;
                } else if (std::strcmp(argv[i], "8196") == 0) {
                    d_remote = ZfpUtil::REMOTE_8196;
                } else {
                    std::cerr << "\nInvalid ZFP port : '" << argv[i] << "'.\n\n";
                    printUsage();
                    return false;
                }
            }
            else if (!std::strcmp(argv[i], "-auth") && i + 1 < argc) {
                ++ i;
                d_manualToken = false;
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

                    d_manualToken = true;
                    d_manualIPAddress.swap(elems[1]);
                    d_manualUserId.swap(elems[2]);
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
            } else {
                printUsage();

                std::cerr << "\nUnexpected option: '" << argv[i] << "'\n\n";
                return false;
            }
        }

        if (d_clientCredentials.empty() || d_clientCredentialsPassword.empty()
                || d_trustMaterial.empty()) {

            printUsage();
            std::cerr << "\nTls options parameters are required.\n\n";
            return false;
        }

        return true;
    }

public:
    ZfpOverLeasedLinesSessionExample()
        : d_authOptions(AUTH_USER)
        , d_manualToken(false)
        , d_remote(ZfpUtil::REMOTE_8194)
    {
    }

    void run(int argc, char **argv)
    {
        if (!parseCommandLine(argc, argv))
            return;

        // TlsOptions are required by any ZFP connection
        TlsOptions tlsOptions
                = TlsOptions::createFromFiles(
                                    d_clientCredentials.c_str(),
                                    d_clientCredentialsPassword.c_str(),
                                    d_trustMaterial.c_str());

        SessionOptions sessionOptions;
        try {
            sessionOptions = ZfpUtil::getZfpOptionsForLeasedLines(
                d_remote, tlsOptions);
        }
        catch (Exception& exc) {
            std::cout << "Encountered an error while getting ZFP options: "
                << exc.description() << std::endl;
            return;
        }

        // Note: ZFP solution requires authorization. The appropriate authentication option
        // must be set here on the 'SessionOptions' before the session is created.
        sessionOptions.setAuthenticationOptions(d_authOptions.c_str());

        Session session(sessionOptions);
        if (!session.start()) {
            std::cerr <<"Failed to start session." << std::endl;
            while(true) {
                Event event;
                int rc = session.tryNextEvent(&event);
                if (rc) {
                    break;
                }
                MessageIterator msgIter(event);
                while (msgIter.next()) {
                    Message msg = msgIter.message();
                    msg.print(std::cerr) << std::endl;
                }
            }
            return;
        }

        std::cout << "Session started successfully." << std::endl;

        // Note: ZFP solution requires authorization, which should be done here
        // before any subscriptions or requests can be made. For examples of
        // how to authorize or get data, please refer to the specific examples.
    }
};

int main(int argc, char **argv)
{
    std::cout << "ZfpOverLeasedLinesSessionExample" << std::endl;
    ZfpOverLeasedLinesSessionExample example;
    try {
        example.run(argc, argv);
    }
    catch (Exception &e) {
        std::cerr << "Library Exception!!! " << e.description()
            << std::endl;
    }

    // wait for enter key to exit application
    std::cout << "Press ENTER to quit" << std::endl;
    char dummy[2];
    std::cin.getline(dummy, 2);
    return 0;
}
