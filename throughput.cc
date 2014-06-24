#include <stdio.h>
#include <vector>
#include <thread>
#include <chrono>
#include <string>
#include <glog/logging.h>
#include "kinetic/kinetic.h"
#include "threadsafe_blocking_connection.h"

using com::seagate::kinetic::client::proto::Message_Algorithm_SHA1;
using kinetic::KineticConnectionFactory;
using namespace std::chrono;

void parse(int argc, char** argv, int &threads, int &keys, int &keysize, kinetic::PersistMode &persist, std::string &con, std::string &con_type, kinetic::ConnectionOptions &options)
{
    threads = 1;
    keys    = 10;
    keysize = 1024;
    con  = "shared";
    con_type = "cpp_client";
    persist = kinetic::PersistMode::WRITE_BACK;
    options.host = "localhost";
    options.port = 8123;
    options.user_id = 1;
    options.hmac_key = "asdfasdf";

    for(int i = 1; i+1 < argc; i++){
        if(strcmp("-threads", argv[i]) == 0)
            threads = std::stoi(argv[i+1]);
        if(strcmp("-keys", argv[i]) == 0)
            keys = std::stoi(argv[i+1]);
        if(strcmp("-keysize", argv[i]) == 0)
            keysize = std::stoi(argv[i+1]);
        if(strcmp("-host", argv[i]) == 0)
            options.host = argv[i+1];
        if(strcmp("-connection", argv[i]) == 0)
            con = argv[i+1];
        if(strcmp("-con_type", argv[i]) == 0)
            con_type = argv[i+1];
        if(strcmp("-persist", argv[i]) == 0){
            if(strcmp(argv[i+1],"write_through")==0)
                persist = kinetic::PersistMode::WRITE_THROUGH;
        }
    }

    printf("configuration of cpp client throughput test (put only): \n"
            "\t-threads %d \n"
            "\t-keys %d \n"
            "\t-keysize %d   \t{size of value in kilobytes} \n"
            "\t-host %s \n"
            "\t-connection %s  \t{shared,unique} \n"
            "\t-con_type %s \t{cpp_client,custom} \n"
            "\t-persist %s \t{write_back,write_through} \n",
            threads, keys, keysize, options.host.c_str(), con.c_str(), con_type.c_str(), persist ==  kinetic::PersistMode::WRITE_BACK ? "write_back" : "write_through");
    keysize*=1024;


}

int main(int argc, char** argv)
{
    google::InitGoogleLogging(argv[0]);
    kinetic::ConnectionOptions options;
    kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
    int num_threads; int num_keys; int value_size; kinetic::PersistMode persist; std::string cuse; std::string ctype;

    parse(argc, argv, num_threads, num_keys, value_size, persist, cuse, ctype, options);

    auto connect = [&](){
        std::shared_ptr<kinetic::BlockingKineticConnection> con;
        kinetic::Status status =  kinetic::Status::makeInternalError("no connection");

        if(ctype.compare("cpp_client")==0) status = factory.NewThreadsafeBlockingConnection(options, con, 5);
        if(ctype.compare("custom")==0){
            std::shared_ptr<kinetic::NonblockingKineticConnection> nblocking;
            status = factory.NewThreadsafeNonblockingConnection(options, nblocking);
            if(status.ok())   con.reset(new kinetic::ThreadsafeBlockingConnection(nblocking, 5));
        }
        if (status.notOk())
            printf("Could not initialize connection\n");
        return con;
    };

    std::shared_ptr<kinetic::BlockingKineticConnection> shared_connection;
    if(cuse.compare("shared")==0) shared_connection = connect();
    std::string value;
    value.resize(value_size, 'X');

    auto test = [&](int tid){

        std::shared_ptr<kinetic::BlockingKineticConnection> connection;
        if(cuse.compare("shared")==0) connection = shared_connection;
        if(cuse.compare("unique")==0) connection = connect();
        if(!connection) return;

        std::string key;
        for(int i=0; i<num_keys; i++){
            key = std::to_string(tid) + "_" + std::to_string(i);
            if(!connection->Put( key, "",
                    kinetic::WriteMode::IGNORE_VERSION,
                    kinetic::KineticRecord(value, std::to_string(i), "", Message_Algorithm_SHA1),
                    persist).ok())
                printf("ERROR DURING PUT \n");
        }
    };

    std::vector<std::thread> threads;

    auto run_start = steady_clock::now();
    for(int i=0; i<num_threads; i++) threads.push_back(std::thread(std::bind(test, i)));
    for(auto & t : threads) t.join();
    auto run_end  = steady_clock::now();

    int  duration = (int) duration_cast<milliseconds>(run_end-run_start).count();

    printf( "\nDone in %d milliseconds "
            "\n\t -->  %f MB/second"
            "\n\t -->  %f keys/second"
            "\n",
            duration,
            (num_threads*num_keys*((float)value.size() / (1024*1024))) / ( duration / 1000.0),
            (num_threads*num_keys) / (duration / 1000.0)
    );


    return 0;
}
