#ifndef BLOCKING_INTERFACE
#define BLOCKING_INTERFACE

#include "kinetic/kinetic.h"

namespace kinetic {

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using std::vector;
using kinetic::ConnectionOptions;
using kinetic::KineticRecord;

class BlockingConnectionInterface{

public:
    virtual ~BlockingConnectionInterface() {};

    virtual KineticStatus NoOp() = 0;
    virtual KineticStatus Get(const string &key, unique_ptr<KineticRecord>& record) = 0;
    virtual KineticStatus Delete(const string &key, const string& version, WriteMode mode) = 0;
    virtual KineticStatus Put(const string &key, const string &current_version, WriteMode mode, const KineticRecord& record) = 0;
    virtual KineticStatus Put(const string &key, const string &current_version, WriteMode mode, const KineticRecord& record, PersistMode persistMode) = 0;
    virtual KineticStatus GetVersion(const string &key, unique_ptr<string>& version) = 0;
    virtual KineticStatus GetLog(unique_ptr<DriveLog>& drive_log) = 0;
    virtual KineticStatus GetKeyRange(const string& start_key, bool start_key_inclusive, const string& end_key, bool end_key_inclusive, bool reverse_results, int32_t max_results, unique_ptr<vector<string>>& keys) = 0;
    virtual KineticStatus SetClusterVersion(int64_t cluster_version) = 0;
    virtual void SetClientClusterVersion(int64_t cluster_version) = 0;

};

}

#endif
