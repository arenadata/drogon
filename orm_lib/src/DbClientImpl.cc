/**
 *
 *  @file DbClientImpl.cc
 *  @author An Tao
 *
 *  Copyright 2018, An Tao.  All rights reserved.
 *  https://github.com/an-tao/drogon
 *  Use of this source code is governed by a MIT license
 *  that can be found in the License file.
 *
 *  Drogon
 *
 */

#include "DbClientImpl.h"
#include "DbConnection.h"
#include "../../lib/src/TaskTimeoutFlag.h"
#include <drogon/config.h>
#include <string_view>
#if USE_POSTGRESQL
#include "postgresql_impl/PgConnection.h"
#endif
#if USE_MYSQL
#include "mysql_impl/MysqlConnection.h"
#endif
#if USE_SQLITE3
#include "sqlite3_impl/Sqlite3Connection.h"
#endif
#include "TransactionImpl.h"
#include <drogon/drogon.h>
#include <drogon/orm/DbClient.h>
#include <drogon/orm/Exception.h>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdio.h>
#ifndef _WIN32
#include <sys/select.h>
#include <unistd.h>
#endif
#include <thread>
#include <trantor/net/EventLoop.h>
#include <trantor/net/Channel.h>
#include <unordered_set>
#include <vector>

using namespace drogon;
using namespace drogon::orm;

DbClientImpl::DbClientImpl(const std::string &connInfo,
                           size_t connNum,
#if LIBPQ_SUPPORTS_BATCH_MODE
                           ClientType type,
                           bool autoBatch)
#else
                           ClientType type)
#endif
    : numberOfConnections_(connNum),
#if LIBPQ_SUPPORTS_BATCH_MODE
      autoBatch_(autoBatch),
#endif
      loops_(type == ClientType::Sqlite3
                 ? 1
                 : (connNum < std::thread::hardware_concurrency()
                        ? connNum
                        : std::thread::hardware_concurrency()),
             "DbLoop")
{
    type_ = type;
    connectionInfo_ = connInfo;
    LOG_TRACE << "DbClientImpl constructor: type=" << (int)type
              << ", connections=" << connNum;
    assert(connNum > 0);
}

void DbClientImpl::init()
{
    LOG_DEBUG << "Initializing DbClientImpl with " << numberOfConnections_
              << " connections";
    loops_.start();
    if (type_ == ClientType::PostgreSQL || type_ == ClientType::Mysql)
    {
        LOG_INFO << "Starting " << numberOfConnections_ << " connections for "
                 << (type_ == ClientType::PostgreSQL ? "PostgreSQL" : "MySQL");
        for (size_t i = 0; i < numberOfConnections_; ++i)
        {
            auto loop = loops_.getNextLoop();
            LOG_TRACE << "Creating connection " << i + 1 << " on loop " << loop;
            loop->runInLoop([this, loop]() { newConnection(loop); });
        }
    }
    else if (type_ == ClientType::Sqlite3)
    {
        LOG_INFO << "Starting " << numberOfConnections_
                 << " connections for SQLite3";
        sharedMutexPtr_ = std::make_shared<SharedMutex>();
        assert(sharedMutexPtr_);

        for (size_t i = 0; i < numberOfConnections_; ++i)
        {
            LOG_TRACE << "Creating SQLite3 connection " << i + 1;
            newConnection(nullptr);
        }
    }
}

DbClientImpl::~DbClientImpl() noexcept
{
    LOG_DEBUG << "DbClientImpl destructor called";
    closeAll();
}

void DbClientImpl::closeAll()
{
    LOG_INFO << "Closing all database connections";
    decltype(connections_) connections;
    {
        std::lock_guard<std::mutex> lock(connectionsMutex_);
        connections.swap(connections_);
        readyConnections_.clear();
        busyConnections_.clear();
    }
    LOG_DEBUG << "Disconnecting " << connections.size() << " connections";
    for (auto const &conn : connections)
    {
        conn->disconnect();
    }
}

void DbClientImpl::execSql(
    const char *sql,
    size_t sqlLength,
    size_t paraNum,
    std::vector<const char *> &&parameters,
    std::vector<int> &&length,
    std::vector<int> &&format,
    ResultCallback &&rcb,
    std::function<void(const std::exception_ptr &)> &&exceptCallback)
{
    LOG_TRACE << "Executing SQL with " << paraNum << " parameters";
    assert(paraNum == parameters.size());
    assert(paraNum == length.size());
    assert(paraNum == format.size());
    assert(rcb);
    if (timeout_ > 0.0)
    {
        LOG_TRACE << "Using timeout of " << timeout_
                  << " seconds for SQL execution";
        execSqlWithTimeout(sql,
                           sqlLength,
                           paraNum,
                           std::move(parameters),
                           std::move(length),
                           std::move(format),
                           std::move(rcb),
                           std::move(exceptCallback));
        return;
    }
    DbConnectionPtr conn;
    bool busy = false;
    {
        std::lock_guard<std::mutex> guard(connectionsMutex_);

        if (readyConnections_.size() == 0)
        {
            if (sqlCmdBuffer_.size() > 200000)
            {
                LOG_ERROR << "Too many queries in buffer ("
                          << sqlCmdBuffer_.size() << "), rejecting new query";
                busy = true;
            }
            else
            {
                LOG_DEBUG
                    << "No ready connections, pushing query to buffer (size: "
                    << sqlCmdBuffer_.size() << ")";
                std::shared_ptr<SqlCmd> cmd =
                    std::make_shared<SqlCmd>(std::string_view{sql, sqlLength},
                                             paraNum,
                                             std::move(parameters),
                                             std::move(length),
                                             std::move(format),
                                             std::move(rcb),
                                             std::move(exceptCallback));
                sqlCmdBuffer_.push_back(std::move(cmd));
            }
        }
        else
        {
            auto iter = readyConnections_.begin();
            busyConnections_.insert(*iter);
            conn = *iter;
            readyConnections_.erase(iter);
            LOG_TRACE << "Using ready connection for SQL execution, ready "
                         "connections: "
                      << readyConnections_.size()
                      << ", busy connections: " << busyConnections_.size();
        }
    }
    if (conn)
    {
        LOG_TRACE << "Executing SQL on connection";
        conn->execSql({sql, sqlLength},
                      paraNum,
                      std::move(parameters),
                      std::move(length),
                      std::move(format),
                      std::move(rcb),
                      std::move(exceptCallback));
        return;
    }
    if (busy)
    {
        LOG_ERROR << "Rejecting SQL execution due to buffer overflow";
        auto exceptPtr =
            std::make_exception_ptr(Failure("Too many queries in buffer"));
        exceptCallback(exceptPtr);
        return;
    }
}

void DbClientImpl::newTransactionAsync(
    const std::function<void(const std::shared_ptr<Transaction> &)> &callback)
{
    LOG_TRACE << "Creating new transaction asynchronously";
    DbConnectionPtr conn;
    {
        std::lock_guard<std::mutex> lock(connectionsMutex_);
        if (!readyConnections_.empty())
        {
            auto iter = readyConnections_.begin();
            busyConnections_.insert(*iter);
            conn = *iter;
            readyConnections_.erase(iter);
            LOG_DEBUG
                << "Using ready connection for transaction, ready connections: "
                << readyConnections_.size()
                << ", busy connections: " << busyConnections_.size();
        }
        else
        {
            LOG_DEBUG << "No ready connections, queueing transaction callback";
            auto callbackPtr = std::make_shared<
                std::function<void(const std::shared_ptr<Transaction> &)>>(
                callback);
            if (timeout_ > 0.0)
            {
                LOG_TRACE << "Setting up transaction timeout of " << timeout_
                          << " seconds";
                auto newCallbackPtr =
                    std::make_shared<std::weak_ptr<std::function<void(
                        const std::shared_ptr<Transaction> &)>>>();
                auto timeoutFlagPtr = std::make_shared<TaskTimeoutFlag>(
                    loops_.getNextLoop(),
                    std::chrono::duration<double>(timeout_),
                    [newCallbackPtr, callbackPtr, this]() {
                        LOG_ERROR << "Transaction creation timed out after "
                                  << timeout_ << " seconds";
                        auto cbPtr = (*newCallbackPtr).lock();
                        if (cbPtr)
                        {
                            std::lock_guard<std::mutex> lock(connectionsMutex_);
                            for (auto iter = transCallbacks_.begin();
                                 iter != transCallbacks_.end();
                                 ++iter)
                            {
                                if (cbPtr == *iter)
                                {
                                    transCallbacks_.erase(iter);
                                    break;
                                }
                            }
                        }
                        (*callbackPtr)(nullptr);
                    });
                callbackPtr = std::make_shared<
                    std::function<void(const std::shared_ptr<Transaction> &)>>(
                    [callbackPtr, timeoutFlagPtr](
                        const std::shared_ptr<Transaction> &trans) {
                        if (timeoutFlagPtr->done())
                            return;
                        (*callbackPtr)(trans);
                    });
                (*newCallbackPtr) = callbackPtr;
                timeoutFlagPtr->runTimer();
            }
            transCallbacks_.push_back(callbackPtr);
        }
    }
    if (conn)
    {
        makeTrans(conn,
                  std::function<void(const std::shared_ptr<Transaction> &)>(
                      callback));
    }
}

void DbClientImpl::makeTrans(
    const DbConnectionPtr &conn,
    std::function<void(const std::shared_ptr<Transaction> &)> &&callback)
{
    LOG_DEBUG << "Creating new transaction on connection";
    std::weak_ptr<DbClientImpl> weakThis = shared_from_this();
    auto trans = std::make_shared<TransactionImpl>(
        type_, conn, std::function<void(bool)>(), [weakThis, conn]() {
            LOG_TRACE << "Transaction callback executed";
            auto thisPtr = weakThis.lock();
            if (!thisPtr)
                return;
            if (conn->status() == ConnectStatus::Bad)
            {
                LOG_ERROR << "Connection is in bad status, cannot reuse";
                return;
            }
            {
                std::lock_guard<std::mutex> guard(thisPtr->connectionsMutex_);
                if (thisPtr->connections_.find(conn) ==
                    thisPtr->connections_.end())
                {
                    LOG_ERROR << "Connection is broken and removed";
                    // connection is broken and removed
                    assert(thisPtr->busyConnections_.find(conn) ==
                               thisPtr->busyConnections_.end() &&
                           thisPtr->readyConnections_.find(conn) ==
                               thisPtr->readyConnections_.end());

                    return;
                }
            }
            conn->loop()->queueInLoop([weakThis, conn]() {
                auto thisPtr = weakThis.lock();
                if (!thisPtr)
                    return;
                std::weak_ptr<DbConnection> weakConn = conn;
                conn->setIdleCallback([weakThis, weakConn]() {
                    auto thisPtr = weakThis.lock();
                    if (!thisPtr)
                        return;
                    auto connPtr = weakConn.lock();
                    if (!connPtr)
                        return;
                    thisPtr->handleNewTask(connPtr);
                });
                thisPtr->handleNewTask(conn);
            });
        });
    trans->doBegin();
    if (timeout_ > 0.0)
    {
        LOG_TRACE << "Setting transaction timeout to " << timeout_
                  << " seconds";
        trans->setTimeout(timeout_);
    }
    conn->loop()->queueInLoop(
        [callback = std::move(callback), trans]() { callback(trans); });
}

std::shared_ptr<Transaction> DbClientImpl::newTransaction(
    const std::function<void(bool)> &commitCallback) noexcept(false)
{
    LOG_DEBUG << "Creating new transaction synchronously";
    std::promise<std::shared_ptr<Transaction>> pro;
    auto f = pro.get_future();
    newTransactionAsync([&pro](const std::shared_ptr<Transaction> &trans) {
        pro.set_value(trans);
    });
    auto trans = f.get();
    if (!trans)
    {
        LOG_ERROR << "Failed to create transaction: timeout or no connection "
                     "available";
        throw TimeoutError("Timeout, no connection available for transaction");
    }
    LOG_TRACE << "Transaction created successfully";
    trans->setCommitCallback(commitCallback);
    return trans;
}

void DbClientImpl::handleNewTask(const DbConnectionPtr &connPtr)
{
    LOG_TRACE << "Handling new task on connection";
    std::function<void(const std::shared_ptr<Transaction> &)> transCallback;
    std::shared_ptr<SqlCmd> cmd;
    {
        std::lock_guard<std::mutex> guard(connectionsMutex_);
        if (!transCallbacks_.empty())
        {
            LOG_DEBUG << "Processing pending transaction callback";
            transCallback = std::move(*(transCallbacks_.front()));
            transCallbacks_.pop_front();
        }
        else if (!sqlCmdBuffer_.empty())
        {
            LOG_DEBUG << "Processing pending SQL command from buffer (size: "
                      << sqlCmdBuffer_.size() << ")";
            cmd = std::move(sqlCmdBuffer_.front());
            sqlCmdBuffer_.pop_front();
        }
        else
        {
            LOG_TRACE << "No pending tasks, marking connection as ready";
            // Connection is idle, put it into the readyConnections_ set;
            busyConnections_.erase(connPtr);
            readyConnections_.insert(connPtr);
        }
    }
    if (transCallback)
    {
        LOG_TRACE << "Executing transaction callback";
        makeTrans(connPtr, std::move(transCallback));
        return;
    }
    if (cmd)
    {
        LOG_TRACE << "Executing SQL command from buffer";
        connPtr->execSql(std::move(cmd->sql_),
                         cmd->parametersNumber_,
                         std::move(cmd->parameters_),
                         std::move(cmd->lengths_),
                         std::move(cmd->formats_),
                         std::move(cmd->callback_),
                         std::move(cmd->exceptionCallback_));
        return;
    }
}

DbConnectionPtr DbClientImpl::newConnection(trantor::EventLoop *loop)
{
    LOG_DEBUG << "Creating new database connection";
    DbConnectionPtr connPtr;
    if (type_ == ClientType::PostgreSQL)
    {
#if USE_POSTGRESQL
#if LIBPQ_SUPPORTS_BATCH_MODE
        LOG_INFO << "Creating new PostgreSQL connection with batch mode "
                 << (autoBatch_ ? "enabled" : "disabled");
        connPtr =
            std::make_shared<PgConnection>(loop, connectionInfo_, autoBatch_);
#else
        LOG_INFO << "Creating new PostgreSQL connection";
        connPtr = std::make_shared<PgConnection>(loop, connectionInfo_, false);
#endif
#else
        LOG_ERROR << "PostgreSQL support not enabled";
        return nullptr;
#endif
    }
    else if (type_ == ClientType::Mysql)
    {
#if USE_MYSQL
        LOG_INFO << "Creating new MySQL connection";
        connPtr = std::make_shared<MysqlConnection>(loop, connectionInfo_);
#else
        LOG_ERROR << "MySQL support not enabled";
        return nullptr;
#endif
    }
    else if (type_ == ClientType::Sqlite3)
    {
#if USE_SQLITE3
        LOG_INFO << "Creating new SQLite3 connection";
        connPtr = std::make_shared<Sqlite3Connection>(loop,
                                                      connectionInfo_,
                                                      sharedMutexPtr_);
#else
        LOG_ERROR << "SQLite3 support not enabled";
        return nullptr;
#endif
    }
    else
    {
        LOG_ERROR << "Unknown database type: " << static_cast<int>(type_);
        return nullptr;
        (void)(loop);
    }
    std::weak_ptr<DbClientImpl> weakPtr = shared_from_this();
    connPtr->setCloseCallback([weakPtr](const DbConnectionPtr &closeConnPtr) {
        LOG_INFO << "Connection closed, removing from connection pools";
        // Erase the connection
        auto thisPtr = weakPtr.lock();
        if (!thisPtr)
            return;
        {
            std::lock_guard<std::mutex> guard(thisPtr->connectionsMutex_);
            thisPtr->readyConnections_.erase(closeConnPtr);
            thisPtr->busyConnections_.erase(closeConnPtr);
            assert(thisPtr->connections_.find(closeConnPtr) !=
                   thisPtr->connections_.end());
            thisPtr->connections_.erase(closeConnPtr);
            LOG_DEBUG
                << "Connection removed from pools, remaining connections: "
                << thisPtr->connections_.size();
        }
        // Reconnect after 1 second
        auto loop = closeConnPtr->loop();
        // closeConnPtr may be not valid. Close the connection file descriptor.
        closeConnPtr->disconnect();
        LOG_INFO << "Scheduling reconnection attempt in 1 second";
        loop->runAfter(1, [weakPtr, loop, closeConnPtr] {
            auto thisPtr = weakPtr.lock();
            if (!thisPtr)
                return;
            LOG_DEBUG << "Attempting to reconnect after connection failure";
            thisPtr->newConnection(loop);
        });
    });

    connPtr->setOkCallback([weakPtr](const DbConnectionPtr &okConnPtr) {
        LOG_INFO << "Database connection established successfully";
        auto thisPtr = weakPtr.lock();
        if (!thisPtr)
            return;
        {
            std::lock_guard<std::mutex> guard(thisPtr->connectionsMutex_);
            thisPtr->busyConnections_.insert(
                okConnPtr);  // For new connections, this sentence is
                             // necessary
            LOG_DEBUG << "Connection added to busy pool, busy connections: "
                      << thisPtr->busyConnections_.size();
        }
        thisPtr->handleNewTask(okConnPtr);
    });

    std::weak_ptr<DbConnection> weakConn = connPtr;
    connPtr->setIdleCallback([weakPtr, weakConn]() {
        LOG_TRACE << "Connection idle callback triggered";
        auto thisPtr = weakPtr.lock();
        if (!thisPtr)
            return;
        auto connPtr = weakConn.lock();
        if (!connPtr)
            return;
        thisPtr->handleNewTask(connPtr);
    });

    {
        std::lock_guard<std::mutex> guard(connectionsMutex_);
        connections_.insert(connPtr);
        LOG_DEBUG << "Connection added to connections pool, total connections: "
                  << connections_.size();
    }

    // Init database connection only after all callbacks are set and connPtr
    // is added to connections_.
    LOG_TRACE << "Initializing database connection";
    connPtr->init();

    return connPtr;
}

bool DbClientImpl::hasAvailableConnections() const noexcept
{
    std::lock_guard<std::mutex> lock(connectionsMutex_);
    bool hasConnections =
        (!readyConnections_.empty()) || (!busyConnections_.empty());
    LOG_TRACE << "Checking for available connections: "
              << (hasConnections ? "available" : "none available")
              << " (ready: " << readyConnections_.size()
              << ", busy: " << busyConnections_.size() << ")";
    return hasConnections;
}

void DbClientImpl::execSqlWithTimeout(
    const char *sql,
    size_t sqlLength,
    size_t paraNum,
    std::vector<const char *> &&parameters,
    std::vector<int> &&length,
    std::vector<int> &&format,
    ResultCallback &&rcb,
    std::function<void(const std::exception_ptr &)> &&ecb)
{
    LOG_TRACE << "Executing SQL with timeout of " << timeout_ << " seconds";
    DbConnectionPtr conn;
    assert(timeout_ > 0.0);
    auto cmd = std::make_shared<std::weak_ptr<SqlCmd>>();
    bool busy = false;
    auto ecpPtr =
        std::make_shared<std::function<void(const std::exception_ptr &)>>(
            std::move(ecb));

    LOG_DEBUG << "Setting up timeout flag for SQL execution";
    auto timeoutFlagPtr = std::make_shared<drogon::TaskTimeoutFlag>(
        loops_.getNextLoop(),
        std::chrono::duration<double>(timeout_),
        [cmd, ecpPtr, thisPtr = shared_from_this(), timeout = timeout_]() {
            LOG_ERROR << "SQL execution timed out after " << timeout
                      << " seconds";
            auto cbPtr = (*cmd).lock();
            if (cbPtr)
            {
                std::lock_guard<std::mutex> lock(thisPtr->connectionsMutex_);
                for (auto iter = thisPtr->sqlCmdBuffer_.begin();
                     iter != thisPtr->sqlCmdBuffer_.end();
                     ++iter)
                {
                    if (*iter == cbPtr)
                    {
                        LOG_DEBUG
                            << "Removing timed out SQL command from buffer";
                        thisPtr->sqlCmdBuffer_.erase(iter);
                        break;
                    }
                }
            }
            (*ecpPtr)(
                std::make_exception_ptr(TimeoutError("SQL execution timeout")));
        });

    auto resultCallback = [rcb = std::move(rcb),
                           timeoutFlagPtr](const Result &result) {
        if (timeoutFlagPtr->done())
        {
            LOG_TRACE << "Ignoring SQL result callback as operation already "
                         "timed out";
            return;
        }
        LOG_TRACE << "SQL execution completed successfully";
        rcb(result);
    };

    auto exceptionCallback = [ecpPtr,
                              timeoutFlagPtr](const std::exception_ptr &err) {
        if (timeoutFlagPtr->done())
        {
            LOG_TRACE << "Ignoring SQL exception callback as operation already "
                         "timed out";
            return;
        }
        LOG_ERROR << "SQL execution failed with exception";
        (*ecpPtr)(err);
    };

    {
        std::lock_guard<std::mutex> guard(connectionsMutex_);

        if (readyConnections_.size() == 0)
        {
            if (sqlCmdBuffer_.size() > 200000)
            {
                LOG_ERROR << "Too many queries in buffer ("
                          << sqlCmdBuffer_.size() << "), rejecting new query";
                busy = true;
            }
            else
            {
                LOG_DEBUG
                    << "No ready connections, pushing query to buffer (size: "
                    << sqlCmdBuffer_.size() << ")";
                auto command =
                    std::make_shared<SqlCmd>(std::string_view{sql, sqlLength},
                                             paraNum,
                                             std::move(parameters),
                                             std::move(length),
                                             std::move(format),
                                             std::move(resultCallback),
                                             std::move(exceptionCallback));
                sqlCmdBuffer_.emplace_back(command);
                *cmd = command;
            }
        }
        else
        {
            auto iter = readyConnections_.begin();
            busyConnections_.insert(*iter);
            conn = *iter;
            readyConnections_.erase(iter);
            LOG_TRACE << "Using ready connection for SQL execution, ready "
                         "connections: "
                      << readyConnections_.size()
                      << ", busy connections: " << busyConnections_.size();
        }
    }
    if (conn)
    {
        LOG_DEBUG << "Executing SQL on connection with timeout";
        conn->execSql(std::string_view{sql, sqlLength},
                      paraNum,
                      std::move(parameters),
                      std::move(length),
                      std::move(format),
                      std::move(resultCallback),
                      std::move(exceptionCallback));
        timeoutFlagPtr->runTimer();
        return;
    }

    if (busy)
    {
        LOG_ERROR << "Rejecting SQL execution due to buffer overflow";
        exceptionCallback(
            std::make_exception_ptr(Failure("Too many queries in buffer")));
        return;
    }

    LOG_TRACE << "Starting timeout timer for queued SQL execution";
    timeoutFlagPtr->runTimer();
}
