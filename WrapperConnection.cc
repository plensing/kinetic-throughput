#include "WrapperConnection.h"
using namespace kinetic;

KineticStatus WrapperConnection::NoOp()
{
    std::lock_guard<std::mutex> guard(mutex);
    return connection_->NoOp();
}

KineticStatus WrapperConnection::Get(const string& key, unique_ptr<KineticRecord>& record)
{
    std::lock_guard<std::mutex> guard(mutex);
    return connection_->Get(key,record);
}

KineticStatus WrapperConnection::Put(const string& key, const string& current_version, WriteMode mode, const KineticRecord& record)
{
    std::lock_guard<std::mutex> guard(mutex);
    return connection_->Put(key,current_version,mode,record);
}

KineticStatus WrapperConnection::Put(const string& key, const string& current_version, WriteMode mode, const KineticRecord& record, PersistMode persistMode)
{
    std::lock_guard<std::mutex> guard(mutex);
    return connection_->Put(key,current_version,mode,record,persistMode);
}

KineticStatus WrapperConnection::Delete(const string& key, const string& version, WriteMode mode)
{
    std::lock_guard<std::mutex> guard(mutex);
    return connection_->Delete(key,version,mode);
}

KineticStatus WrapperConnection::GetVersion(const string &key, unique_ptr<string>& version)
{
    std::lock_guard<std::mutex> guard(mutex);
    return connection_->GetVersion(key,version);
}

KineticStatus WrapperConnection::GetKeyRange(const string& start_key, bool start_key_inclusive, const string& end_key,
        bool end_key_inclusive, bool reverse_results, int32_t max_results, unique_ptr<vector<string>>& keys)
{
    std::lock_guard<std::mutex> guard(mutex);
    return connection_->GetKeyRange(start_key,start_key_inclusive, end_key,
            end_key_inclusive, reverse_results, max_results, keys);
}

KineticStatus WrapperConnection::SetClusterVersion(int64_t cluster_version)
{
    std::lock_guard<std::mutex> guard(mutex);
    return connection_->SetClusterVersion(cluster_version);
}

KineticStatus WrapperConnection::GetLog(unique_ptr<DriveLog>& drive_log)
{
    std::lock_guard<std::mutex> guard(mutex);
    return connection_->GetLog(drive_log);
}

void WrapperConnection::SetClientClusterVersion(int64_t cluster_version)
{
    std::lock_guard<std::mutex> guard(mutex);
    return connection_->SetClientClusterVersion(cluster_version);
}


WrapperConnection::WrapperConnection(const ConnectionOptions &options)
{
    kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
    factory.NewBlockingConnection(options, connection_,60);
}

WrapperConnection::~WrapperConnection()
{
}



