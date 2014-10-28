#include <stdio.h>
#include <vector>
#include <thread>
#include <chrono>
#include <string>
#include "ActiveListenerConnection.h"
#include <glog/logging.h>

using kinetic::KineticConnectionFactory;
using kinetic::KineticRecord;
using kinetic::WriteMode;
using std::shared_ptr;
using std::vector;
using std::string;
using namespace std::chrono;

enum class contype{ ORIG, CUSTOM };
enum class conselect{ HASH, FIXED };
struct configuration{
    int num_threads;
    int num_keys;
    int value_size;
    kinetic::PersistMode persist;
    contype con;
    conselect select;
    vector<string> hosts;
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
             if(strcmp(argv[i+1],"custom")==0)
                 config.con = contype::CUSTOM;
        }
        if(strcmp("-select", argv[i]) == 0){
             if(strcmp(argv[i+1],"fixed")==0)
                 config.select = conselect::FIXED;
        }
    }

    printf("configuration of cpp client throughput test: \n");
    for( auto h : config.hosts)
        printf( "\t-host %s    \t{can be used multiple times} \n",h.c_str());
    printf( "\t-threads %d   \t\t{number of threads concurrently putting values}\n"
            "\t-keys %d        \t{number of keys put by each thread} \n"
            "\t-size %d      \t\t{size of value in kilobytes} \n"
            "\t-persist %s   \t{write_back,write_through} \n"
            "\t-con %s       \t{standard,custom}\n"
            "\t-select %s    \t{hash,fixed}\n",
            config.num_threads, config.num_keys, config.value_size,
            config.persist ==  kinetic::PersistMode::WRITE_BACK ? "write_back" : "write_through",
            config.con == contype::ORIG ? "standard" : "custom",
            config.select == conselect::HASH ? "hash" : "fixed"
                    );
    config.value_size*=1024;
}


enum class OperationType{
    PUT, GET, DEL
};
string to_str(OperationType type){
    switch(type){
    case OperationType::GET: return("GET");
    case OperationType::PUT: return("PUT");
    case OperationType::DEL: return("DEL");
    }
    return ("invalid");
}

void connect(const configuration &config, vector<shared_ptr<kinetic::BlockingKineticConnectionInterface>> &cons)
{
    auto listener = shared_ptr<kinetic::ConnectionListener>(new kinetic::ConnectionListener());
    kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
    shared_ptr<kinetic::BlockingKineticConnectionInterface> con;

    for(auto h : config.hosts){
       kinetic::ConnectionOptions options;
       options.host = h;
       options.port = 8123;
       options.user_id = 1;
       options.hmac_key = "asdfasdf";
       options.use_ssl = false;

       if(config.con == contype::ORIG){
           shared_ptr<kinetic::ThreadsafeBlockingKineticConnection> ocon;
           factory.NewThreadsafeBlockingConnection(options, ocon, 5);
           con = ocon;
       }
       else if(config.con == contype::CUSTOM){
        try{
           con.reset(new kinetic::ActiveListenerConnection(options,listener));
        }
        catch(std::exception & e){
            printf("Exception: %s",e.what());
        }
       }
       if(con)
           cons.push_back(con);
    }
}

int main(int argc, char** argv)
{
    google::InitGoogleLogging("");
    struct configuration config = {1,100,0,kinetic::PersistMode::WRITE_BACK,contype::ORIG,conselect::HASH,{}};
    parse(argc, argv, config);

    vector<shared_ptr<kinetic::BlockingKineticConnectionInterface>> cons;
    connect(config,cons);
    if(cons.empty()){
        printf("\n No Connection // Specify -host \n");
        exit(0);
    }

    string value;
    value.resize(config.value_size, 'X');

    auto test = [&](int tid, OperationType type){

        string key;
        int connectionID = tid % cons.size();

        for(int i=0; i<config.num_keys; i++){
            key = std::to_string(tid) + "_" + std::to_string(i);
            if(config.select == conselect::HASH)
                connectionID = std::hash<string>()(key) % cons.size();
            kinetic::KineticStatus status = kinetic::KineticStatus(kinetic::StatusCode::REMOTE_OTHER_ERROR, "");

            switch(type){
            case OperationType::PUT:{
                    KineticRecord record(value, std::to_string(i), "", com::seagate::kinetic::client::proto::Command_Algorithm_SHA1);
                    status = cons[connectionID]->Put(key, "", WriteMode::IGNORE_VERSION, record, config.persist);
                }
                break;
            case OperationType::GET:{
                    std::unique_ptr<KineticRecord> record;
                    status = cons[connectionID]->Get(key, record);
                }
                break;
            case OperationType::DEL:{
                    status = cons[connectionID]->Delete(key, "", WriteMode::IGNORE_VERSION, config.persist);
                }
                break;
            }
            if(!status.ok())
                 printf("ERROR DURING %s OPERATION: %s \n",to_str(type).c_str(), status.message().c_str());
        }
    };


    OperationType types[] = {OperationType::PUT, OperationType::GET, OperationType::DEL};

    for(auto t : types){
        vector<std::thread> threads;
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
