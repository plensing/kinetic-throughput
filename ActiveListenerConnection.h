#ifndef ACTIVE_LISTENER_CONNECTION
#define ACTIVE_LISTENER_CONNECTION

#include "kinetic/kinetic.h"

namespace kinetic {

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using std::vector;
using std::mutex;

class ConnectionListener {
private:
    vector<shared_ptr<ThreadsafeNonblockingKineticConnection>> *connections;
    bool *run;

    int  pipeFD_receive;
    int  pipeFD_send;
    mutex lock;

public:
    explicit ConnectionListener();
    ~ConnectionListener();

    bool con_add(shared_ptr<ThreadsafeNonblockingKineticConnection> con);
    bool con_remove(shared_ptr<ThreadsafeNonblockingKineticConnection> con);
    void poke();
};


/* Kinetic connection class variant that implements an independent socket listener in order to provide
 * blocking functionality for multi-threaded scenarios. Only implements subset of blocking kinetic interface. */
class ActiveListenerConnection : public BlockingKineticConnectionInterface {

private:
    const KineticStatus invalid = KineticStatus(StatusCode::CLIENT_INTERNAL_ERROR,"operation not supported",0);

    std::shared_ptr<ConnectionListener> listener_;
    std::shared_ptr<ThreadsafeNonblockingKineticConnection> nonblocking_connection_;

    void connect(const ConnectionOptions &options);

public:
    explicit ActiveListenerConnection(const ConnectionOptions &options);
    explicit ActiveListenerConnection(const ConnectionOptions &options, std::shared_ptr<ConnectionListener> listener);
    ~ActiveListenerConnection();

    void SetClientClusterVersion(int64_t cluster_version);

    KineticStatus NoOp();
    KineticStatus Get(const string &key, unique_ptr<KineticRecord>& record);
    KineticStatus Delete(const string &key, const string& version, WriteMode mode);
    KineticStatus Delete(const string& key, const string& version, WriteMode mode, PersistMode persistMode);
    KineticStatus Put(const string &key, const string &current_version, WriteMode mode, const KineticRecord& record);
    KineticStatus Put(const string &key, const string &current_version, WriteMode mode, const KineticRecord& record, PersistMode persistMode);
    KineticStatus GetVersion(const string &key, unique_ptr<string>& version);
    KineticStatus GetLog(unique_ptr<DriveLog>& drive_log);
    KineticStatus GetLog(const vector<Command_GetLog_Type>& types, unique_ptr<DriveLog>& drive_log);
    KineticStatus GetKeyRange(const string& start_key, bool start_key_inclusive, const string& end_key, bool end_key_inclusive, bool reverse_results, int32_t max_results, unique_ptr<vector<string>>& keys);
    KineticStatus SetClusterVersion(int64_t cluster_version);
    KineticStatus InstantErase(const string& pin);


    // Didn't bother to implement below methods. 
    KineticStatus Get(const shared_ptr<const string> key,unique_ptr<KineticRecord>& record){return invalid;}
    KineticStatus GetNext(const shared_ptr<const string> key,unique_ptr<string>& actual_key,
            unique_ptr<KineticRecord>& record){return invalid;}
    KineticStatus GetNext(const string& key, unique_ptr<string>& actual_key,
            unique_ptr<KineticRecord>& record){return invalid;}
    KineticStatus GetPrevious(const shared_ptr<const string> key, unique_ptr<string>& actual_key,
            unique_ptr<KineticRecord>& record){return invalid;}
    KineticStatus GetPrevious(const string& key, unique_ptr<string>& actual_key,
            unique_ptr<KineticRecord>& record){return invalid;}
    KineticStatus GetVersion(const shared_ptr<const string> key, unique_ptr<string>& version){return invalid;}
    KineticStatus GetKeyRange(const shared_ptr<const string> start_key,
               bool start_key_inclusive,
               const shared_ptr<const string> end_key,
               bool end_key_inclusive,
               bool reverse_results,
               int32_t max_results,
               unique_ptr<vector<string>>& keys){return invalid;}
    KeyRangeIterator IterateKeyRange(const shared_ptr<const string> start_key,bool start_key_inclusive,
            const shared_ptr<const string> end_key, bool end_key_inclusive,
            unsigned int frame_size){return KeyRangeIterator();}
    KeyRangeIterator IterateKeyRange(const string& start_key, bool start_key_inclusive, const string& end_key,
            bool end_key_inclusive, unsigned int frame_size){return KeyRangeIterator();}
    KineticStatus Put(const shared_ptr<const string> key,
            const shared_ptr<const string> current_version, WriteMode mode,
            const shared_ptr<const KineticRecord> record,
            PersistMode persistMode){return invalid;}
    KineticStatus Put(const shared_ptr<const string> key,
            const shared_ptr<const string> current_version, WriteMode mode,
            const shared_ptr<const KineticRecord> record){return invalid;}
    KineticStatus Delete(const shared_ptr<const string> key,
            const shared_ptr<const string> version, WriteMode mode, PersistMode persistMode){return invalid;}
    KineticStatus Delete(const shared_ptr<const string> key,
            const shared_ptr<const string> version, WriteMode mode){return invalid;}
    KineticStatus P2PPush(const P2PPushRequest& push_request,
            unique_ptr<vector<KineticStatus>>& operation_statuses){return invalid;}
    KineticStatus P2PPush(const shared_ptr<const P2PPushRequest> push_request,
            unique_ptr<vector<KineticStatus>>& operation_statuses){return invalid;}
    KineticStatus UpdateFirmware(const shared_ptr<const string> new_firmware){return invalid;}
    KineticStatus SetACLs(const shared_ptr<const list<ACL>> acls){return invalid;}
    KineticStatus SetErasePIN(const shared_ptr<const string> new_pin,
            const shared_ptr<const string> current_pin = make_shared<string>()){return invalid;}
    KineticStatus SetErasePIN(const string& new_pin, const string& current_pin){return invalid;}
    KineticStatus SetLockPIN(const shared_ptr<const string> new_pin,
            const shared_ptr<const string> current_pin = make_shared<string>()){return invalid;}
    KineticStatus SetLockPIN(const string& new_pin, const string& current_pin){return invalid;}
    KineticStatus InstantErase(const shared_ptr<string> pin){return invalid;}
    KineticStatus SecureErase(const shared_ptr<string> pin){return invalid;}
    KineticStatus SecureErase(const string& pin){return invalid;}
    KineticStatus LockDevice(const shared_ptr<string> pin){return invalid;};
    KineticStatus LockDevice(const string& pin){return invalid;}
    KineticStatus UnlockDevice(const shared_ptr<string> pin){return invalid;}
    KineticStatus UnlockDevice(const string& pin){return invalid;}

};

} // namespace kinetic


#endif  // ACTIVE_LISTENER_CONNECTION
