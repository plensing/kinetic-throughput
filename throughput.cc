#include <stdio.h>
#include <vector>
#include <thread>
#include <chrono>
#include <string>
#include "threadsafe_blocking_connection.h"


#define SHA1 com::seagate::kinetic::client::proto::Message_Algorithm_SHA1
// #define SHA1 com::seagate::kinetic::client::proto::Command_Algorithm_SHA1

using kinetic::KineticConnectionFactory;
using kinetic::KineticRecord;
using kinetic::WriteMode;
using namespace std::chrono;

enum class contype{ ORIG, SHARED, MULTI };
struct configuration{
    int num_threads;
    int num_keys;
    int value_size;
    kinetic::PersistMode persist;
    contype con;
    std::vector<std::string> hosts;
};

void parse(int argc, char** argv, configuration &config)
{
    for(int i = 1; i+1 < argc; i++){
        if(strcmp("-threads", argv[i]) == 0)
            config.num_threads = std::stoi(argv[i+1]);
        if(strcmp("-keys", argv[i]) == 0)
            config.num_keys = std::stoi(argv[i+1]);
        if(strcmp("-size", argv[i]) == 0)
            config.value_size = std::stoi(argv[i+1]);
        if(strcmp("-host", argv[i]) == 0)
            config.hosts.push_back(argv[i+1]);
        if(strcmp("-persist", argv[i]) == 0){
            if(strcmp(argv[i+1],"write_through")==0)
                config.persist = kinetic::PersistMode::WRITE_THROUGH;
        }
        if(strcmp("-con", argv[i]) == 0){
             if(strcmp(argv[i+1],"orig")==0)
                 config.con = contype::ORIG;
             else if(strcmp(argv[i+1],"multi")==0)
                 config.con = contype::MULTI;
        }
    }

    printf("configuration of cpp client throughput test: \n");
    for( auto h : config.hosts)
        printf( "\t-host %s    \t{can be used multiple times} \n",h.c_str());
    printf( "\t-threads %d   \t\t{number of threads concurrently putting values}\n"
            "\t-keys %d        \t{number of keys put by each threads} \n"
            "\t-size %d      \t\t{size of value in kilobytes} \n"
            "\t-persist %s  \t{write_back,write_through} \n"
            "\t-con %s    \t\t{orig,shared,multi}\n",
            config.num_threads, config.num_keys, config.value_size,
            config.persist ==  kinetic::PersistMode::WRITE_BACK ? "write_back" : "write_through",
            config.con == contype::ORIG ? "orig" : config.con == contype::SHARED ? "shared" : "multi"
                    );
    config.value_size*=1024;
}


enum class OperationType{
    PUT, GET, DEL
};
std::string to_str(OperationType type){
    switch(type){
    case OperationType::GET: return("GET");
    case OperationType::PUT: return("PUT");
    case OperationType::DEL: return("DEL");
    }
    return ("invalid");
}

void connect(const configuration &config,
        std::vector<std::shared_ptr<kinetic::BlockingKineticConnection>> &conso,
        std::vector<std::shared_ptr<kinetic::ThreadsafeBlockingConnection>> &consc)
{
    auto listener = std::shared_ptr<kinetic::ConnectionListener>(new kinetic::ConnectionListener());
    for(auto h : config.hosts){
       kinetic::ConnectionOptions options;
       options.host = h;
       options.port = 8123;
       options.user_id = 1;
       options.hmac_key = "asdfasdf";

       if(config.con == contype::ORIG){
           std::shared_ptr<kinetic::BlockingKineticConnection> con;
           kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
           factory.NewThreadsafeBlockingConnection(options, con, 5);
           if(con)
           conso.push_back(con);
       }
       else{
           std::shared_ptr<kinetic::ThreadsafeBlockingConnection> con;
           if(config.con == contype::MULTI)
               con.reset(new kinetic::ThreadsafeBlockingConnection(options));
           else if(config.con == contype::SHARED)
               con.reset(new kinetic::ThreadsafeBlockingConnection(options, listener));
           if(con)
           consc.push_back(con);
       }
    }
}

int main(int argc, char** argv)
{
    struct configuration config = {1,100,0,kinetic::PersistMode::WRITE_BACK,contype::SHARED,{}};
    parse(argc, argv, config);

    std::vector<std::shared_ptr<kinetic::BlockingKineticConnection>> conso;
    std::vector<std::shared_ptr<kinetic::ThreadsafeBlockingConnection>> consc;
    connect(config,conso,consc);
    if(conso.empty() && consc.empty()){
        printf("\n No Connection // Specify -host \n");
        exit(0);
    }

    std::string value;
    value.resize(config.value_size, 'X');

    auto test = [&](int tid, OperationType type){

        std::string key;
        int connectionID=0;

        for(int i=0; i<config.num_keys; i++){
            key = std::to_string(tid) + "_" + std::to_string(i);
            connectionID = std::hash<std::string>()(key) % (config.con == contype::ORIG ? conso.size() : consc.size());
            kinetic::KineticStatus status = kinetic::KineticStatus(kinetic::StatusCode::REMOTE_OTHER_ERROR, "");

            switch(type){
            case OperationType::PUT:{
                    KineticRecord record(value, std::to_string(i), "", SHA1);
                    if(config.con == contype::ORIG) status = conso[connectionID]->Put(key, "", WriteMode::IGNORE_VERSION, record , config.persist);
                    else status = consc[connectionID]->Put(key, "", WriteMode::IGNORE_VERSION, record , config.persist);
                }
                break;
            case OperationType::GET:{
                    std::unique_ptr<KineticRecord> record;
                    if(config.con == contype::ORIG) status = conso[connectionID]->Get( key, record);
                    else status = consc[connectionID]->Get(key, record);
                }
                break;
            case OperationType::DEL:{
                    if(config.con == contype::ORIG) status = conso[connectionID]->Delete(key, "", WriteMode::IGNORE_VERSION);
                    else status = consc[connectionID]->Delete(key, "", WriteMode::IGNORE_VERSION);
                }
                break;
            }
            if(!status.ok())
                 printf("ERROR DURING %s OPERATION: %s \n",to_str(type).c_str(), status.message().c_str());
        }
    };



    OperationType types[] = {OperationType::PUT, OperationType::GET, OperationType::DEL};

    for(auto t : types){
        std::vector<std::thread> threads;
        auto run_start = steady_clock::now();

        for(int i=0; i<config.num_threads; i++)
            threads.push_back(std::thread(std::bind(test, i, t)));
        for(auto & t : threads)
            t.join();

        auto run_end  = steady_clock::now();
        int  duration = (int) duration_cast<milliseconds>(run_end-run_start).count();

        printf( "\n%s done in %d milliseconds "
                "\n\t -->  %f MB/second"
                "\n\t -->  %f keys/second"
                "\n",
                to_str(t).c_str(),duration,
                (config.num_threads*config.num_keys*((float)value.size() / (1024*1024))) / ( duration / 1000.0),
                (config.num_threads*config.num_keys) / (duration / 1000.0)
        );
    }
    return 0;
}
