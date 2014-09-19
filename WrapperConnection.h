#ifndef WRAPPERCONNECTION
#define WRAPPERCONNECTION

#include "ConnectionInterface.h"

namespace kinetic {

class WrapperConnection : public BlockingConnectionInterface{

private:
    std::unique_ptr<BlockingKineticConnection> connection_;
    std::mutex mutex;

public:
    explicit WrapperConnection(const ConnectionOptions &options);
    ~WrapperConnection();

    KineticStatus NoOp();
    KineticStatus Get(const string &key, unique_ptr<KineticRecord>& record);
    KineticStatus Delete(const string &key, const string& version, WriteMode mode);
    KineticStatus Put(const string &key, const string &current_version, WriteMode mode, const KineticRecord& record);
    KineticStatus Put(const string &key, const string &current_version, WriteMode mode, const KineticRecord& record, PersistMode persistMode);
    KineticStatus GetVersion(const string &key, unique_ptr<string>& version);
    KineticStatus GetLog(unique_ptr<DriveLog>& drive_log);
    KineticStatus GetKeyRange(const string& start_key, bool start_key_inclusive, const string& end_key, bool end_key_inclusive, bool reverse_results, int32_t max_results, unique_ptr<vector<string>>& keys);
    KineticStatus SetClusterVersion(int64_t cluster_version);

    void SetClientClusterVersion(int64_t cluster_version);

}; // namespace kinetic


}


#endif
