#include <stdio.h>
#include <vector>
#include <thread>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <random>
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

struct Range {
  int start;
  int end;

  int size() const {
    return end - start;
  }

  Range() : start(0), end(0) {};
  Range(int start, int end) : start(start), end(end) {};
};

struct configuration {
  // user configuration
  std::string host;
  int port;
  int security_id;
  string security_key;

  int report_keys;
  int random_sequence_size;
  int random_seed;
  bool prefix_compressible;
  Range keys;
  Range key_size;
  Range value_size;

  std::string req_version;
  std::string new_version;

  kinetic::PersistMode persist;
  bool flush_on_report;

  int queue_depth;

  vector<OperationType> ops;

  configuration() :
     host("nohost"), port(8123), security_id(1), security_key("asdfasdf"),
     report_keys(0), random_sequence_size(0), random_seed(123), prefix_compressible(false), keys(0,1),
     persist(kinetic::PersistMode::WRITE_BACK), flush_on_report(false), queue_depth(1)
  {}
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
  /* Parse set options */
  for(int i = 1; i+1 < argc; i++){
    if(strcmp("-keys", argv[i]) == 0)
      config.keys.end = std::stoi(argv[i+1]);
    if(strcmp("-keys_start", argv[i]) == 0)
      config.keys.start = std::stoi(argv[i+1]);
    if(strcmp("-keys_end", argv[i]) == 0)
      config.keys.end = std::stoi(argv[i+1]);
    if(strcmp("-key_size", argv[i]) == 0)
      config.key_size = {std::stoi(argv[i+1]),std::stoi(argv[i+1])};
    if(strcmp("-key_size_start", argv[i]) == 0)
      config.key_size.start = std::stoi(argv[i+1]);
    if(strcmp("-key_size_end", argv[i]) == 0)
      config.key_size.end = std::stoi(argv[i+1]);
    if(strcmp("-value_size", argv[i]) == 0)
      config.value_size = {std::stoi(argv[i+1]),std::stoi(argv[i+1])};
    if(strcmp("-value_size_start", argv[i]) == 0)
      config.value_size.start = std::stoi(argv[i+1]);
    if(strcmp("-value_size_end", argv[i]) == 0)
      config.value_size.end = std::stoi(argv[i+1]);
    if(strcmp("-queue_depth", argv[i]) == 0)
      config.queue_depth = std::stoi(argv[i+1]);
    if(strcmp("-host", argv[i]) == 0)
      config.host = argv[i+1];
    if(strcmp("-port", argv[i]) == 0)
      config.port = std::stoi(argv[i+1]);
    if(strcmp("-report", argv[i]) == 0)
      config.report_keys = std::stoi(argv[i+1]);
    if(strcmp("-id", argv[i]) == 0)
      config.security_id = std::stoi(argv[i+1]);
    if(strcmp("-pw", argv[i]) == 0)
      config.security_key = string(argv[i+1]);
    if(strcmp("-req_version", argv[i]) == 0)
      config.req_version = string(argv[i+1]);
    if(strcmp("-new_version", argv[i]) == 0)
      config.new_version = string(argv[i+1]);
    if(strcmp("-ran_seq", argv[i]) == 0)
      config.random_sequence_size = std::stoi(argv[i+1]);
    if(strcmp("-seed", argv[i]) == 0)
      config.random_seed = std::stoi(argv[i+1]);
    if(strcmp("-persist", argv[i]) == 0){
      if(strcmp(argv[i+1],"write_through")==0)
        config.persist = kinetic::PersistMode::WRITE_THROUGH;
    }
    if(strcmp("-flush", argv[i]) == 0){
      if(strcmp(argv[i+1],"enabled")==0)
        config.flush_on_report = true;
    }
    if(strcmp("-prefix", argv[i]) == 0){
      if(strcmp(argv[i+1],"true")==0)
        config.prefix_compressible = true;
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

  /* Sanitize set options */
  if(!config.report_keys || config.report_keys > config.keys.size())
    config.report_keys = config.keys.size();
  if(!config.random_sequence_size)
    config.random_sequence_size = config.keys.size();
  if(config.key_size.start < 10)
    config.key_size.start = 10;
  if(config.key_size.end < config.key_size.start)
    config.key_size.end = config.key_size.start;
  if(config.value_size.end < config.value_size.start)
    config.value_size.end = config.value_size.start;

  /* Report set options */
  printf("------------------------------------------------------------------------------ \n");
  printf("                cpp client throughput test               \n");
  printf("------------------------------------------------------------------------------ \n");
  printf("  Selected operation sequence {put, get, del}: ");
  for(size_t i = 0; i < config.ops.size(); i++) {
    printf(" -op %s", to_str(config.ops[i]).c_str());
  }
  printf("\n\n  Selected host: -host %s\n"
            "  Port number used for connection: -port %d\n"
            "  Identity used for connection: -id %d \n"
            "  Secret key used for connection: -pw %s \n"
            "  \n"
            "  Generate keys prefix compressible (to keys_start): -prefix {true, false}: %s \n"
            "  Key range: -keys [_start, _end]: [%d, %d]\n"
            "  Key sizes in byte: -key_size [_start, _end]: [%d, %d]\n"
            "  \n"
            "  Randomize access order every n sequential operations: -ran_seq %d \n"
            "  Randomization Seed: -seed %d \n"
            "  \n"
            "  Queue Depth for requests: -queue_depth %d \n"
            "  Report average performance ever n keys: -report %d \n"
            "  \n"
            "  PUT / DELETE ONLY: \n"
            "  Flush connection before reporting {enabled,disabled}: -flush %s \n"
            "  Persistence {write_back,write_through}: -persist %s \n"
            "  Value sizes in KB: -value_size [_start, _end]: [%d, %d] \n"
            "  Require key-version: -req_version %s \n"
            "  Set key-version: -new_version %s \n",
          config.host.c_str(), config.port, config.security_id, config.security_key.c_str(),

          config.prefix_compressible ? "true" : "false",
          config.keys.start, config.keys.end,
          config.key_size.start, config.key_size.end,

          config.random_sequence_size, config.random_seed,

          config.queue_depth, config.report_keys,

          config.flush_on_report ? "enabled" : "disabled",
          config.persist ==  kinetic::PersistMode::WRITE_BACK ? "write_back" : "write_through",
          config.value_size.start, config.value_size.end,
          config.req_version.c_str(), config.new_version.c_str()
          );
  printf("------------------------------------------------------------------------------ \n");
  config.value_size.start*=1024;
  config.value_size.end*=1024;
}
void test(
          int numOperations, OperationType operationType,
          std::list<uint32_t>::iterator& operationKeys,
          configuration& config,
          std::unique_ptr <kinetic::ThreadsafeNonblockingKineticConnection>& con,
          size_t& value_size)
{
  std::mt19937 size_engine;
  std::uniform_int_distribution<int> key_size_distribution(config.key_size.start, config.key_size.end);
  std::uniform_int_distribution<int> value_size_distribution(config.value_size.start, config.value_size.end);
  auto sync = std::make_shared<kio::CallbackSynchronization>();

  for (int i = 0; i < numOperations; i++) {
    size_engine.seed(*operationKeys);
    size_t request_key_size = key_size_distribution(size_engine);

    std::ostringstream ss;

    if(config.prefix_compressible){
      ss << std::setw(config.key_size.start - 7) << std::setfill('0') << 0;
      ss << std::setw(7)  << std::setfill('0') <<  *operationKeys;
      ss << std::setw(request_key_size - config.key_size.start) << std::setfill('0') << 0;
    } else{
      ss << std::setw(7)  << std::setfill('0') <<  *operationKeys;
      ss << std::setw(request_key_size - 7) << std::setfill('0') << 0;
    }
    std::string key = ss.str();
    //printf("key=%ld, key_size=%ld, value_size=%ld, keyname=%s\n",*operationKeys, request_key_size, request_value_size, key.c_str());
    *operationKeys++;

    switch (operationType) {
      case OperationType::PUT: {
        size_t request_value_size = value_size_distribution(size_engine);
        auto cb = std::make_shared<kio::PutCallback>(sync);
        auto record = std::make_shared<KineticRecord>(
            std::string(request_value_size, 'X'),
            config.new_version,
            std::to_string((long long int) i),
            com::seagate::kinetic::client::proto::Command_Algorithm_SHA1
        );
        if(config.req_version.length()) {
          con->Put(key, config.req_version, WriteMode::REQUIRE_SAME_VERSION, record, cb, config.persist);
        }
        else{
          con->Put(key, "", WriteMode::IGNORE_VERSION, record, cb, config.persist);
        }
        value_size += request_value_size;
      }
        break;
      case OperationType::GET: {
        auto cb = std::make_shared<kio::GetCallback>(sync);
        con->Get(key, cb);
      }
        break;
      case OperationType::DEL: {
        auto cb = std::make_shared<kio::BasicCallback>(sync);
        if(config.req_version.length()) {
          con->Delete(key, config.req_version, WriteMode::REQUIRE_SAME_VERSION, cb, config.persist);
        }
        else{
          con->Delete(key, "", WriteMode::IGNORE_VERSION, cb, config.persist);
        }
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

  if(operationType == OperationType::GET) {
    value_size = sync->getRequestSize();
  }
}

std::list<uint32_t> constructAccessList(struct configuration& config)
{
  std::vector<std::list<uint32_t>> sequence_vector(config.keys.size() / config.random_sequence_size);

  for (size_t sequence = 0; sequence < sequence_vector.size(); sequence++) {
    for (size_t number = sequence * config.random_sequence_size + config.keys.start;
         number < (sequence + 1) * config.random_sequence_size + config.keys.start; number++) {
      sequence_vector[sequence].push_back(number);
    }
  }

  std::mt19937 sequence_engine(config.random_seed);
  std::shuffle(sequence_vector.begin(), sequence_vector.end(), sequence_engine);

  std::list<uint32_t> access_list;
  for (size_t sequence = 0; sequence < sequence_vector.size(); sequence++) {
    access_list.splice(access_list.begin(), sequence_vector[sequence]);
  }
  return access_list;
}

int main(int argc, char** argv)
{
  configuration config;
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
    auto num_runs = config.keys.size() / config.report_keys;
    auto ops_per_run = config.keys.size() / num_runs;

    std::list<uint32_t>::iterator cursor = access_list.begin();

    for (int r = 0; r < num_runs; r++) {
      size_t value_size = 0;

      auto run_start = system_clock::now();
      test(ops_per_run, op, cursor, config, con, value_size);
      auto run_end = system_clock::now();
      int duration = (int) duration_cast<milliseconds>(run_end - run_start).count();

      printf("\n%s of %d keys done in %d milliseconds, total value size %.2f MB "
                 "\n\t Values -->  %.2f MB/second"
                 "\n\t Operations -->  %.2f keys/second"
                 "\n",
             to_str(op).c_str(), config.report_keys, duration, ((float)value_size) / (1024*1024),
             ((float) value_size / (1024 * 1024)) / (duration / 1000.0),
             config.report_keys / (duration / 1000.0)
      );
      fflush(stdout);
    }
  }
  return 0;
}
