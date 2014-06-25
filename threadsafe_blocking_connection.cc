#include "threadsafe_blocking_connection.h"
#include <mutex>
#include <condition_variable>
#include <fcntl.h>
#include <unistd.h>
using namespace kinetic;

class ThreadsafeBlockingCallbackState {
private:
    std::condition_variable cv;
    std::mutex    mutex;
    KineticStatus status;
    bool done;

    public:
    ThreadsafeBlockingCallbackState() :
        status(KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR, "no result")),
        done(false)
        {};
    virtual ~ThreadsafeBlockingCallbackState() {}

    void OnResult(KineticStatus result) {
        std::unique_lock<std::mutex> lck(mutex);
        status = result;
        done   = true;
        lck.unlock();
        cv.notify_all();
    }

    /* wait on condition variable if work isn't done yet. */
    KineticStatus getResult() {
        std::unique_lock<std::mutex> lck(mutex);
        while(!done)
            cv.wait(lck);
        return status;
    }
};

class SimpleCallback : public SimpleCallbackInterface, public PutCallbackInterface, public ThreadsafeBlockingCallbackState {
    public:
    virtual void Success() {
        OnResult(KineticStatus(StatusCode::OK, ""));
    }
    virtual void Failure(KineticStatus error) {
        OnResult(error);
    }
};

class GetCallback : public GetCallbackInterface, public ThreadsafeBlockingCallbackState {
    public:
    GetCallback( unique_ptr<string>& actual_key, unique_ptr<KineticRecord>& record, bool want_actual_key)
        : actual_key_(actual_key), record_(record), want_actual_key_(want_actual_key) {}

    virtual void Success(const string &key, unique_ptr<KineticRecord> record) {
        if (want_actual_key_) {
            if (actual_key_) *actual_key_ = key;
            else actual_key_.reset(new string(key));
        }
        record_ = std::move(record);
        OnResult(KineticStatus(StatusCode::OK, ""));
    }

    virtual void Failure(KineticStatus error) {
        OnResult(error);
    }

    private:
    unique_ptr<string>& actual_key_;
    unique_ptr<KineticRecord>& record_;
    bool want_actual_key_;
};

class GetVersionCallback : public GetVersionCallbackInterface, public ThreadsafeBlockingCallbackState {
    public:
    explicit GetVersionCallback(unique_ptr<string>& version)
    : version_(version) {}

    virtual void Success(const std::string& version) {
        if (version_) *version_ = version;
        else           version_.reset(new string(version));

        OnResult(KineticStatus(StatusCode::OK, ""));
    }

    virtual void Failure(KineticStatus error) {
        OnResult(error);
    }

    private:
    unique_ptr<string>& version_;
};

class GetKeyRangeCallback : public GetKeyRangeCallbackInterface, public ThreadsafeBlockingCallbackState {
    public:
    explicit GetKeyRangeCallback(unique_ptr<vector<string>>& keys)
    :  keys_(keys) {}

    virtual void Success(unique_ptr<vector<string>> keys) {
        keys_ = move(keys);
        OnResult(KineticStatus(StatusCode::OK, ""));
    }

    virtual void Failure(KineticStatus error) {
        OnResult(error);
    }

    private:
    unique_ptr<vector<string>>& keys_;
};

class GetLogCallback : public GetLogCallbackInterface, public ThreadsafeBlockingCallbackState {
    public:
    explicit GetLogCallback(unique_ptr<DriveLog>& drive_log) :  drive_log_(drive_log) {}

    virtual void Success(unique_ptr<DriveLog> drive_log) {
        drive_log_ = std::move(drive_log);
        OnResult(KineticStatus(StatusCode::OK, ""));
    }
    virtual void Failure(KineticStatus error) {
        OnResult(error);
    }

    private:
    unique_ptr<DriveLog>& drive_log_;
};

void ThreadsafeBlockingConnection::good_morning()
{
    write(pipeFD_send,"1",1);
}

KineticStatus ThreadsafeBlockingConnection::NoOp()
{
    auto callback = make_shared<SimpleCallback>();
    nonblocking_connection_->NoOp(callback);

    good_morning();
    return callback->getResult();
}

KineticStatus ThreadsafeBlockingConnection::Get(const string& key, unique_ptr<KineticRecord>& record)
{
    unique_ptr<string> actual_key(nullptr);
    auto callback = make_shared<GetCallback>(actual_key, record, false);
    nonblocking_connection_->Get(key, callback);

    good_morning();
    return callback->getResult();
}


KineticStatus ThreadsafeBlockingConnection::Put(const string& key, const string& current_version, WriteMode mode, const KineticRecord& record)
{
    auto callback = make_shared<SimpleCallback>();
    nonblocking_connection_->Put(key, current_version, mode, make_shared<KineticRecord>(record), callback);

    good_morning();
    return callback->getResult();
}

KineticStatus ThreadsafeBlockingConnection::Put(const string& key, const string& current_version, WriteMode mode, const KineticRecord& record, PersistMode persistMode)
{
    auto callback = make_shared<SimpleCallback>();
    nonblocking_connection_->Put(key, current_version, mode, make_shared<KineticRecord>(record), callback, persistMode);

    good_morning();
    return callback->getResult();
}



KineticStatus ThreadsafeBlockingConnection::Delete(const string& key, const string& version, WriteMode mode)
{
    auto callback = make_shared<SimpleCallback>();
    nonblocking_connection_->Delete(key, version, mode, callback);

    good_morning();
    return callback->getResult();
}

KineticStatus ThreadsafeBlockingConnection::GetVersion(const string &key, unique_ptr<string>& version)
{
    auto callback = make_shared<GetVersionCallback>(version);
    nonblocking_connection_->GetVersion(key, callback);

    good_morning();
    return callback->getResult();
}

KineticStatus ThreadsafeBlockingConnection::GetKeyRange(const string& start_key, bool start_key_inclusive, const string& end_key,
        bool end_key_inclusive, bool reverse_results, int32_t max_results, unique_ptr<vector<string>>& keys)
{
    auto callback = make_shared<GetKeyRangeCallback>(keys);
    nonblocking_connection_->GetKeyRange(start_key, start_key_inclusive, end_key, end_key_inclusive, reverse_results, max_results, callback);

    good_morning();
    return callback->getResult();
}

KineticStatus ThreadsafeBlockingConnection::SetClusterVersion(int64_t cluster_version)
{
    auto callback = make_shared<SimpleCallback>();
    nonblocking_connection_->SetClusterVersion(cluster_version, callback);

    good_morning();
    return callback->getResult();
}

KineticStatus ThreadsafeBlockingConnection::GetLog(unique_ptr<DriveLog>& drive_log)
{
    auto callback = make_shared<GetLogCallback>(drive_log);
    nonblocking_connection_->GetLog(callback);

    good_morning();
    return callback->getResult();
}

void ThreadsafeBlockingConnection::SetClientClusterVersion(int64_t cluster_version)
{
    nonblocking_connection_->SetClientClusterVersion(cluster_version);
}


void slisten(
        std::shared_ptr<NonblockingKineticConnection> con,
        unsigned int timeout_seconds,
        int pipeFD, bool &run)
{
    fd_set read_fds, write_fds;
    int num_fds = 0;
    char buf[1];
    while(run){
        con->Run(&read_fds, &write_fds, &num_fds);

        /* add pipe fd so that we can wake up select from the blocking API. */
        FD_SET(pipeFD, &read_fds);
        select(std::max(num_fds,pipeFD) + 1, &read_fds, &write_fds, NULL, NULL);

        /* clear pipe wakeup-fd */
        read(pipeFD, buf, 1);
    }
}


ThreadsafeBlockingConnection::ThreadsafeBlockingConnection(
    std::shared_ptr<NonblockingKineticConnection> nonblocking_connection,
    unsigned int network_timeout_seconds) :
            BlockingKineticConnection(nonblocking_connection, network_timeout_seconds),
            nonblocking_connection_(nonblocking_connection), run_listener(true)
{
    int pFD[2];
    int err = pipe(pFD);
    if (err) throw std::runtime_error("Failed creating PIPE. Errno "+std::to_string(err));

    pipeFD_receive  = pFD[0];
    pipeFD_send     = pFD[1];
    fcntl(pipeFD_receive, F_SETFL, O_NONBLOCK);
    fcntl(pipeFD_send, F_SETFL, O_NONBLOCK);


    std::thread( std::bind(slisten,
            nonblocking_connection_,
            network_timeout_seconds,
            pipeFD_receive, std::ref(run_listener))).detach();

}

ThreadsafeBlockingConnection::~ThreadsafeBlockingConnection()
{
    run_listener = false;
    good_morning();
}



