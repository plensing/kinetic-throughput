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

    private:
    void good_morning();

    std::shared_ptr<NonblockingKineticConnection> nonblocking_connection_;
    bool run_listener;
    int  pipeFD_receive;
    int  pipeFD_send;

    KineticStatus GetKineticStatus(StatusCode code);
    DISALLOW_COPY_AND_ASSIGN(ThreadsafeBlockingConnection);
    };

} // namespace kinetic




#endif  // KINETIC_CPP_CLIENT_THREADSAFE_BLOCKING_CONNECTION_H_
