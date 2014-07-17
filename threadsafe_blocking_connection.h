/* h-flat file system: Hierarchical Functionality in a Flat Namespace
 * Copyright (c) 2014 Seagate
 * Written by Paul Hermann Lensing
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef KINETIC_CPP_CLIENT_THREADSAFE_BLOCKING_CONNECTION_H_
#define KINETIC_CPP_CLIENT_THREADSAFE_BLOCKING_CONNECTION_H_

#include "kinetic/blocking_kinetic_connection.h"
#include <thread>

namespace kinetic {

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using std::vector;

/// Kinetic connection class variant that implements an independent socket listener in order to provide
/// blocking functionality for multi-threaded scenarios
class ThreadsafeBlockingConnection : public BlockingKineticConnection {

private:
    std::shared_ptr<NonblockingKineticConnection> nonblocking_connection_;
    bool run_listener;
    int  pipeFD_receive;
    int  pipeFD_send;
    KineticStatus invalid;

public:
    KineticStatus NoOp();
    KineticStatus Get(const string &key, unique_ptr<KineticRecord>& record);
    KineticStatus Delete(const string &key, const string& version, WriteMode mode);
    KineticStatus Put(const string &key, const string &current_version, WriteMode mode, const KineticRecord& record);
    KineticStatus Put(const string &key, const string &current_version, WriteMode mode, const KineticRecord& record, PersistMode persistMode);
    KineticStatus GetVersion(const string &key, unique_ptr<string>& version);
    KineticStatus GetKeyRange(const string& start_key, bool start_key_inclusive, const string& end_key, bool end_key_inclusive, bool reverse_results, int32_t max_results, unique_ptr<vector<string>>& keys);

    KineticStatus SetClusterVersion(int64_t cluster_version);
    KineticStatus GetLog(unique_ptr<DriveLog>& drive_log);

    void SetClientClusterVersion(int64_t cluster_version);

    explicit ThreadsafeBlockingConnection(
        std::shared_ptr<NonblockingKineticConnection> nonblocking_connection,
        unsigned int network_timeout_seconds);
    virtual ~ThreadsafeBlockingConnection();


    /* Functionality not used by h-flat is left unimplemented. */
public:
    KineticStatus Get(const shared_ptr<const string> key, unique_ptr<KineticRecord>& record){return invalid;}
    KineticStatus GetNext(const shared_ptr<const string> key, unique_ptr<string>& actual_key, unique_ptr<KineticRecord>& record){return invalid;}
    KineticStatus GetNext(const string& key, unique_ptr<string>& actual_key,  unique_ptr<KineticRecord>& record){return invalid;}
    KineticStatus GetPrevious(const shared_ptr<const string> key, unique_ptr<string>& actual_key, unique_ptr<KineticRecord>& record){return invalid;}
    KineticStatus GetPrevious(const string& key, unique_ptr<string>& actual_key, unique_ptr<KineticRecord>& record){return invalid;}
    KineticStatus GetVersion(const shared_ptr<const string> key, unique_ptr<string>& version){return invalid;}
    KineticStatus GetKeyRange(const shared_ptr<const string> start_key, bool start_key_inclusive, const shared_ptr<const string> end_key, bool end_key_inclusive, bool reverse_results, int32_t max_results, unique_ptr<vector<string>>& keys){return invalid;}
    KineticStatus Put(const shared_ptr<const string> key, const shared_ptr<const string> current_version, WriteMode mode, const shared_ptr<const KineticRecord> record, PersistMode persistMode){return invalid;}
    KineticStatus Put(const shared_ptr<const string> key, const shared_ptr<const string> current_version, WriteMode mode, const shared_ptr<const KineticRecord> record){return invalid;}
    KineticStatus Delete(const shared_ptr<const string> key, const shared_ptr<const string> version, WriteMode mode, PersistMode persistMode){return invalid;}
    KineticStatus Delete(const string& key, const string& version, WriteMode mode, PersistMode persistMode){return invalid;}
    KineticStatus Delete(const shared_ptr<const string> key, const shared_ptr<const string> version, WriteMode mode){return invalid;}
    KineticStatus InstantSecureErase(const shared_ptr<string> pin){return invalid;}
    KineticStatus InstantSecureErase(const string& pin){return invalid;}
    KineticStatus UpdateFirmware(const shared_ptr<const string> new_firmware){return invalid;}
    KineticStatus SetACLs(const shared_ptr<const list<ACL>> acls){return invalid;}
    KineticStatus SetPin(const shared_ptr<const string> new_pin, const shared_ptr<const string> current_pin = make_shared<string>()){return invalid;}
    KineticStatus SetPin(const string& new_pin, const string& current_pin){return invalid;}
    KineticStatus P2PPush(const P2PPushRequest& push_request, unique_ptr<vector<KineticStatus>>& operation_statuses){return invalid;}
    KineticStatus P2PPush(const shared_ptr<const P2PPushRequest> push_request, unique_ptr<vector<KineticStatus>>& operation_statuses){return invalid;}
    KeyRangeIterator IterateKeyRange(const shared_ptr<const string> start_key, bool start_key_inclusive, const shared_ptr<const string> end_key, bool end_key_inclusive, unsigned int frame_size){KeyRangeIterator k; return k;}
    KeyRangeIterator IterateKeyRange(const string& start_key, bool start_key_inclusive, const string& end_key, bool end_key_inclusive, unsigned int frame_size){KeyRangeIterator k; return k;}


    private:
    void good_morning();
    KineticStatus GetKineticStatus(StatusCode code);
    DISALLOW_COPY_AND_ASSIGN(ThreadsafeBlockingConnection);
    };

} // namespace kinetic




#endif  // KINETIC_CPP_CLIENT_THREADSAFE_BLOCKING_CONNECTION_H_
