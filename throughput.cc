#include <stdio.h>
#include <vector>
#include <thread>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include "kinetic/kinetic.h"
#include "KineticCallbacks.hh"

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

struct configuration {
  std::string host;
  int security_id;
  string security_key;

  int start_key;
  int num_keys;
  int report_keys;
  int random_sequence_size;

  int put_value_size;
  int put_key_size;

  kinetic::PersistMode persist;
  bool flush_on_report;
  int queue_depth;

  vector<OperationType> ops;
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
    if(strcmp("-keys", argv[i]) == 0)
      config.num_keys = std::stoi(argv[i+1]);
    if(strcmp("-starting_key", argv[i]) == 0)
      config.start_key = std::stoi(argv[i+1]);
    if(strcmp("-key_size", argv[i]) == 0)
      config.put_key_size = std::stoi(argv[i+1]);
    if(strcmp("-value_size", argv[i]) == 0)
      config.put_value_size = std::stoi(argv[i+1]);
    if(strcmp("-queue_depth", argv[i]) == 0)
      config.queue_depth = std::stoi(argv[i+1]);
    if(strcmp("-host", argv[i]) == 0)
      config.host = argv[i+1];
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
    if(strcmp("-flush", argv[i]) == 0){
      if(strcmp(argv[i+1],"enabled")==0)
        config.flush_on_report = true;
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

  if(!config.report_keys || config.report_keys > config.num_keys)
    config.report_keys = config.num_keys;
  if(!config.random_sequence_size)
    config.random_sequence_size = config.num_keys;

  printf("---------------------------------------------------------- \n");
  printf("                cpp client throughput test               \n");
  printf("---------------------------------------------------------- \n");
  printf("\n  Selected operation sequence {from put, get, del, log}: ");
  for(size_t i = 0; i < config.ops.size(); i++) {
    printf(" -op %s", to_str(config.ops[i]).c_str());
  }
  printf( "\n  Selected hosts: -host %s\n"
            "  Queue Depth for requests: -queue_depth %d \n"
            "  Secret key used for connection: -key %s \n"
            "  Identity used for connection: -id %d \n"
            "  Number of keys: -keys %d \n"
            "  Starting key number: -starting_key %d\n"
            "  Size of key in Byte: -key_size %d \n"
            "  Size of value in KB: -value_size %d \n"
            "  Report average performance ever n keys: -report %d \n"
            "  Flush connection before reporting {enabled,disabled}: -flush %s \n"
            "  Put / Delete persistence {write_back,write_through}: -persist %s \n"
            "  Randomize key every n sequential operations: -ran_seq %d \n",
          config.host.c_str(), config.queue_depth, config.security_key.c_str(), config.security_id,
          config.num_keys, config.start_key, config.put_key_size, config.put_value_size, config.report_keys,
          config.flush_on_report ? "enabled" : "disabled",
          config.persist ==  kinetic::PersistMode::WRITE_BACK ? "write_back" : "write_through",
          config.random_sequence_size);
  printf("---------------------------------------------------------- \n");
  config.put_value_size*=1024;
}
void test(
          int numOperations, OperationType operationType,
          std::list<uint32_t>::iterator& operationKeys,
          const configuration& config,
          std::unique_ptr <kinetic::ThreadsafeNonblockingKineticConnection>& con)
{
  string value(config.put_value_size, 'X');
  auto sync = std::make_shared<kio::CallbackSynchronization>();

  for (int i = 0; i < numOperations; i++) {

    std::ostringstream ss;
    ss << std::setw(config.put_key_size) << std::setfill('0') << *operationKeys++;
    auto key = ss.str();



    kinetic::KineticStatus status = kinetic::KineticStatus(kinetic::StatusCode::REMOTE_OTHER_ERROR, "");
    switch (operationType) {
      case OperationType::PUT: {
        auto cb = std::make_shared<kio::PutCallback>(sync);
        auto record = std::make_shared<KineticRecord>(value,
                             std::to_string((long long int) i), std::to_string((long long int) i),
                             com::seagate::kinetic::client::proto::Command_Algorithm_SHA1);
        con->Put(key, "", WriteMode::IGNORE_VERSION, record, cb, config.persist);
      }
        break;
      case OperationType::GET: {
        auto cb = std::make_shared<kio::GetCallback>(sync);
        con->Get(key, cb);
      }
        break;
      case OperationType::DEL: {
        auto cb = std::make_shared<kio::BasicCallback>(sync);
        con->Delete(key, "", WriteMode::IGNORE_VERSION, cb, config.persist);
      }
        break;
      case OperationType::LOG: {
        auto cb = std::make_shared<kio::GetLogCallback>(sync);
        con->GetLog({kinetic::Command_GetLog_Type::Command_GetLog_Type_UTILIZATIONS}, cb);
      }
        break;
    }
    sync->run_until(con, config.queue_depth);
  }

  if (config.flush_on_report) {
    auto cb = std::make_shared<kio::BasicCallback>(sync);
    con->Flush(cb);
  }
  sync->run_until(con, 1);
}

std::list<uint32_t> constructAccessList(const struct configuration& config) {

  std::vector<std::list<uint32_t>> sequence_vector(config.num_keys / config.random_sequence_size);
  for (size_t sequence = 0; sequence < sequence_vector.size(); sequence++) {
    for (size_t number = sequence * config.random_sequence_size + config.start_key;
         number < (sequence + 1) * config.random_sequence_size + config.start_key; number++) {
      sequence_vector[sequence].push_back(number);
    }
  }
  std::random_shuffle(sequence_vector.begin(), sequence_vector.end());

  std::list<uint32_t> access_list;
  for (size_t sequence = 0; sequence < sequence_vector.size(); sequence++) {
    access_list.splice(access_list.begin(), sequence_vector[sequence]);
  }
  return access_list;
}

int main(int argc, char** argv)
{
  configuration config = {"localhost",1,"asdfasdf",0,100,0,0,1024,16, kinetic::PersistMode::WRITE_BACK, false, 1, {}};
  parse(argc, argv, config);

  kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
  kinetic::ConnectionOptions options;
  options.host = config.host;
  options.port = 8123;
  options.user_id = config.security_id;
  options.hmac_key = config.security_key;
  options.use_ssl = false;

  std::unique_ptr <kinetic::ThreadsafeNonblockingKineticConnection> con;
  factory.NewThreadsafeNonblockingConnection(options, con);

  if(!con){
    printf("\n No connection. \n");
    exit(0);
  }

  auto access_list = constructAccessList(config);

  for (size_t o = 0; o < config.ops.size(); o++) {

    auto op = config.ops[o];
    auto num_runs = config.num_keys / config.report_keys;
    auto ops_per_run = config.num_keys / num_runs;

    std::vector<std::list<uint32_t>::iterator> thread_iterators;
    std::list<uint32_t>::iterator cursor = access_list.begin();

    thread_iterators.push_back(cursor);
    std::advance(cursor, config.num_keys);

    for (int r = 0; r < num_runs; r++) {

      auto run_start = system_clock::now();
      test(ops_per_run, op, cursor, config, con);
      auto run_end = system_clock::now();
      int duration = (int) duration_cast<milliseconds>(run_end - run_start).count();

      printf("\n%s of %d keys done in %d milliseconds "
                 "\n\t Values -->  %f MB/second"
                 "\n\t Operations -->  %f keys/second"
                 "\n",
             to_str(op).c_str(), config.report_keys, duration,
             (config.report_keys * ((float) config.put_value_size / (1024 * 1024))) / (duration / 1000.0),
             config.report_keys / (duration / 1000.0)
      );
      fflush(stdout);
    }
  }
  return 0;
}
