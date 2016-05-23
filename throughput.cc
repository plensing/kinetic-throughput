#include <stdio.h>
#include <vector>
#include <thread>
#include <sstream>
#include <iomanip>
#include "kinetic/kinetic.h"

using kinetic::KineticConnectionFactory;
using kinetic::KineticRecord;
using kinetic::WriteMode;
using std::shared_ptr;
using std::vector;
using std::string;
using namespace std::chrono;

enum class OperationType {
  PUT, GET, DEL, LOG
};
enum class conselect{
  HASH, FIXED
};

struct configuration{
  int num_threads;
  int num_keys;
  int value_size;
  int report_keys;
  kinetic::PersistMode persist;
  conselect select;
  vector<string> hosts;
  vector<OperationType> ops;
  int security_id;
  string security_key;
  int random_sequence_size;
};

string to_str(OperationType type){
  switch(type){
    case OperationType::GET: return("GET");
    case OperationType::PUT: return("PUT");
    case OperationType::DEL: return("DEL");
    case OperationType::LOG: return("LOG");
  }
  return "INVALID";
}

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
    if(strcmp("-report", argv[i]) == 0)
      config.report_keys = std::stoi(argv[i+1]);
    if(strcmp("-id", argv[i]) == 0)
      config.security_id = std::stoi(argv[i+1]);
    if(strcmp("-key", argv[i]) == 0)
      config.security_key = string(argv[i+1]);
    if(strcmp("-ran_seq", argv[i]) == 0)
      config.random_sequence_size = std::stoi(argv[i+1]);
    if(strcmp("-persist", argv[i]) == 0){
      if(strcmp(argv[i+1],"write_through")==0)
        config.persist = kinetic::PersistMode::WRITE_THROUGH;
    }
    if(strcmp("-select", argv[i]) == 0){
      if(strcmp(argv[i+1],"fixed")==0)
        config.select = conselect::FIXED;
    }
    if(strcmp("-op", argv[i]) == 0){
      if(strcmp(argv[i+1],"put")==0)
        config.ops.push_back(OperationType::PUT);
      if(strcmp(argv[i+1],"get")==0)
        config.ops.push_back(OperationType::GET);
      if(strcmp(argv[i+1],"del")==0)
        config.ops.push_back(OperationType::DEL);
      if(strcmp(argv[i+1],"log")==0)
        config.ops.push_back(OperationType::LOG);
    }
  }

  if(!config.report_keys)
    config.report_keys = config.num_keys;

  printf("---------------------------------------------------------- \n");
  printf("                cpp client throughput test               \n");
  printf("---------------------------------------------------------- \n");
  printf("  Selected hosts: ");
  for(size_t i = 0; i < config.hosts.size(); i++)
      printf( " -host %s",config.hosts[i].c_str());
  printf("\n  Selected operation sequence {from put, get, del, log}: ");
  for(size_t i = 0; i < config.ops.size(); i++)
    printf(" -op %s", to_str(config.ops[i]).c_str());
  printf( "\n  Secret key used for connection(s): -key %s \n"
            "  Identity used for connection(s): -id %d \n"
            "  Number of threads: -threads %d \n"
            "  Number of keys: -keys %d \n"
            "  Size of keys in KB: -size %d \n"
            "  Report average performance ever n keys: -report %d \n"
            "  Kinetic persistence: -persist %s   \n"
            "  Selection strategy for connections {hash,fixed}: -select %s \n"
            "  Randomize key every n sequential operations, a value of 0 disables randomization: -ran_seq %d \n",
          config.security_key.c_str(), config.security_id,
          config.num_threads, config.num_keys, config.value_size, config.report_keys,
          config.persist ==  kinetic::PersistMode::WRITE_BACK ? "write_back" : "write_through",
          config.select == conselect::HASH ? "hash" : "fixed",
          config.random_sequence_size);
          config.value_size*=1024;
  printf("---------------------------------------------------------- \n");
}



void connect(const configuration &config, vector<shared_ptr<kinetic::BlockingKineticConnectionInterface>> &cons)
{
  kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();

  for(size_t i=0; i<config.hosts.size(); i++){
    kinetic::ConnectionOptions options;
    options.host = config.hosts[i];
    options.port = 8123;
    options.user_id = config.security_id;
    options.hmac_key = config.security_key;
    options.use_ssl = false;

    shared_ptr<kinetic::ThreadsafeBlockingKineticConnection> con;
    factory.NewThreadsafeBlockingConnection(options, con, 300);
    if(con)
      cons.push_back(con);
  }
}

void test(int tid, int start_key, OperationType type, const configuration& config, vector<shared_ptr<kinetic::BlockingKineticConnectionInterface>> &cons)
{

  int key_base = 0;
  string value;
  value.resize(config.value_size, 'X');

  int connectionID = tid % cons.size();
  int keys_per_thread = config.report_keys / config.num_threads;

  for(int i=0; i<keys_per_thread; i++){
    int block_number = start_key+i;

    std::ostringstream ss;
    if(config.random_sequence_size && (block_number % config.random_sequence_size == 0)) {
      key_base = rand();
    }
    ss << std::setw(10) << std::setfill('0') << key_base;
    ss << std::setw(10) << std::setfill('0') << block_number;
    auto key = ss.str();

    if(block_number < 10)
      printf("debug key %d is %s\n",block_number,key.c_str());

    if(config.select == conselect::HASH)
      connectionID = std::hash<string>()(key) % cons.size();

    kinetic::KineticStatus status = kinetic::KineticStatus(kinetic::StatusCode::REMOTE_OTHER_ERROR, "");

    switch(type){
      case OperationType::PUT:{
        KineticRecord record(value, std::to_string((long long int)i), "", com::seagate::kinetic::client::proto::Command_Algorithm_SHA1);
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
      case OperationType::LOG:{
        std::unique_ptr<kinetic::DriveLog> log;
        status = cons[connectionID]->GetLog({kinetic::Command_GetLog_Type::Command_GetLog_Type_UTILIZATIONS}, log);
      }
        break;
    }
    if(!status.ok())
      printf("ERROR DURING %s OPERATION: %s \n",to_str(type).c_str(), status.message().c_str());
  }
}

int main(int argc, char** argv)
{
  struct configuration config = {1,100,0,0,kinetic::PersistMode::WRITE_BACK,conselect::FIXED,{},{},1,"asdfasdf",0};
  parse(argc, argv, config);

  vector<shared_ptr<kinetic::BlockingKineticConnectionInterface>> cons;
  connect(config,cons);
  if(cons.empty() || config.ops.empty()){
    printf("\n Invalid configuration // Specify -host and -op \n");
    exit(0);
  }

  for(size_t o=0; o<config.ops.size(); o++){

    auto op = config.ops[o];

    auto num_runs = config.num_keys / config.report_keys;
    for(int r = 0; r < num_runs; r++){

      vector<std::thread> threads;
      auto run_start = system_clock::now();

      for(int i=0; i<config.num_threads; i++){
        int start_key = i*(config.report_keys/config.num_threads) + r*config.report_keys;
        threads.push_back(std::thread(std::bind(test, i, start_key, op, std::cref(config), std::ref(cons))));
      }
      for(size_t t = 0; t<threads.size(); t++)
        threads[t].join();

      auto run_end  = system_clock::now();
      int  duration = (int) duration_cast<milliseconds>(run_end-run_start).count();

      printf( "\n%s of %d keys done in %d milliseconds "
                  "\n\t -->  %f MB/second"
                  "\n\t -->  %f keys/second"
                  "\n",
              to_str(op).c_str(),config.report_keys, duration,
              (config.report_keys*((float)config.value_size / (1024*1024))) / ( duration / 1000.0),
              config.report_keys / (duration / 1000.0)
      );
      fflush(stdout);
    }
  }
  return 0;
}
