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
typedef std::shared_ptr<kinetic::BlockingKineticConnection> ConnectionPointer;

struct configuration{
    int num_threads;
    int num_keys;
    int value_size;
    bool custom_connection;
    bool random_distribution;
    kinetic::PersistMode persist;
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
        if(strcmp("-custom_connection", argv[i]) == 0){
             if(strcmp(argv[i+1],"false")==0)
                 config.custom_connection = false;
        }
        if(strcmp("-random_distribution", argv[i]) == 0){
             if(strcmp(argv[i+1],"false")==0)
                 config.random_distribution = false;
        }
    }

    printf("configuration of cpp client throughput test (put only): \n\n");
    for( auto h : config.hosts)
        printf( "\t-host %s    \t{can be used multiple times} \n",h.c_str());
    printf( "\t-threads %d   \t\t{number of threads concurrently putting values}\n"
            "\t-keys %d        \t{number of keys put by each threads} \n"
            "\t-size %d      \t\t{size of value in kilobytes} \n"
            "\t-persist %s     \t{write_back,write_through} \n"
            "\t-custom_connection %s  \t{custom or original}\n"
            "\t-random_distribution %s \t{random or round robin} \n",
            config.num_threads, config.num_keys, config.value_size,
            config.persist ==  kinetic::PersistMode::WRITE_BACK ? "write_back" : "write_through",
            config.custom_connection ? "true":"false", config.random_distribution ? "true":"false"
                    );
    config.value_size*=1024;
}

ConnectionPointer connect(kinetic::KineticConnectionFactory &factory, std::string &host, bool custom_connection)
{
    ConnectionPointer con;
    kinetic::ConnectionOptions options;
    kinetic::Status status =  kinetic::Status::makeInternalError("no connection");

    options.host = host;
    options.port = 8123;
    options.user_id = 1;
    options.hmac_key = "asdfasdf";

    if(!custom_connection) status = factory.NewThreadsafeBlockingConnection(options, con, 5);
    else{
       std::shared_ptr<kinetic::NonblockingKineticConnection> nblocking;
       status = factory.NewThreadsafeNonblockingConnection(options, nblocking);
       if(status.ok()) con.reset(new kinetic::ThreadsafeBlockingConnection(nblocking, 5));
    }
    if (status.notOk())
       printf("Could not initialize connection\n");
    return std::move(con);
}


int main(int argc, char** argv)
{
    struct configuration config = {1,10,1024,true,true,kinetic::PersistMode::WRITE_BACK,{}};
    google::InitGoogleLogging(argv[0]);
    kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
    parse(argc, argv, config);

    std::vector<ConnectionPointer> connections;
    for(auto h : config.hosts)
        connections.push_back(connect(factory, h, config.custom_connection));
    if(connections.empty()){
        printf("\n No Connection // Specify -host \n");
        exit(0);
    }

    std::string value;
    value.resize(config.value_size, 'X');

    auto test = [&](int tid){

        std::string key;
        int connectionID=0;

        for(int i=0; i<config.num_keys; i++){
            key = std::to_string(tid) + "_" + std::to_string(i);

            if(config.random_distribution) connectionID = std::hash<std::string>()(key) % connections.size();
            else connectionID = (connectionID+1) % connections.size();


            kinetic::KineticStatus status = connections[connectionID]->Put( key, "",
                    kinetic::WriteMode::IGNORE_VERSION,
                    kinetic::KineticRecord(value, std::to_string(i), "", Message_Algorithm_SHA1),
                    config.persist);

            if(!status.ok())
                printf("ERROR DURING PUT: %s \n",status.message().c_str());
        }
    };


    std::vector<std::thread> threads;

    auto run_start = steady_clock::now();
    for(int i=0; i<config.num_threads; i++) threads.push_back(std::thread(std::bind(test, i)));
    for(auto & t : threads) t.join();
    auto run_end  = steady_clock::now();

    int  duration = (int) duration_cast<milliseconds>(run_end-run_start).count();

    printf( "\nDone in %d milliseconds "
            "\n\t -->  %f MB/second"
            "\n\t -->  %f keys/second"
            "\n",
            duration,
            (config.num_threads*config.num_keys*((float)value.size() / (1024*1024))) / ( duration / 1000.0),
            (config.num_threads*config.num_keys) / (duration / 1000.0)
    );


    return 0;
}
