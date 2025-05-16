/**
 *
 *  @file PgConnection.cc
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

#include "PgConnection.h"
#include "PostgreSQLResultImpl.h"
#include <drogon/orm/Exception.h>
#include <drogon/utils/Utilities.h>
#include <string_view>
#include <trantor/utils/Logger.h>
#include <memory>
#include <stdio.h>

using namespace drogon;
using namespace drogon::orm;

namespace drogon
{
namespace orm
{
Result makeResult(std::shared_ptr<PGresult> &&r = nullptr)
{
    return Result(std::make_shared<PostgreSQLResultImpl>(std::move(r)));
}

}  // namespace orm
}  // namespace drogon

int PgConnection::flush()
{
    auto ret = PQflush(connectionPtr_.get());
    LOG_TRACE << "PQflush returned " << ret;
    if (ret == 1)
    {
        if (!channel_.isWriting())
        {
            LOG_TRACE << "Enabling writing on channel";
            channel_.enableWriting();
        }
    }
    else if (ret == 0)
    {
        if (channel_.isWriting())
        {
            LOG_TRACE << "Disabling writing on channel";
            channel_.disableWriting();
        }
    }
    else if (ret < 0)
    {
        LOG_ERROR << "PQflush error: " << PQerrorMessage(connectionPtr_.get());
    }

    return ret;
}

PgConnection::PgConnection(trantor::EventLoop *loop,
                           const std::string &connInfo,
                           bool)
    : DbConnection(loop),
      connectionPtr_(
          std::shared_ptr<PGconn>(PQconnectStart(connInfo.c_str()),
                                  [](PGconn *conn) { PQfinish(conn); })),
      channel_(loop, PQsocket(connectionPtr_.get()))
{
    LOG_DEBUG << "Creating PostgreSQL connection";
    if (channel_.fd() < 0)
    {
        LOG_ERROR << "Failed to create PostgreSQL connection, invalid socket: "
                  << PQerrorMessage(connectionPtr_.get());
    }
    else
    {
        LOG_DEBUG << "PostgreSQL connection socket created: " << channel_.fd();
    }
}

void PgConnection::init()
{
    LOG_DEBUG << "Initializing PostgreSQL connection";
    if (channel_.fd() < 0)
    {
        LOG_ERROR << "Connection with PostgreSQL could not be established: "
                  << PQerrorMessage(connectionPtr_.get());
        if (closeCallback_)
        {
            auto thisPtr = shared_from_this();
            closeCallback_(thisPtr);
        }
        return;
    }

    LOG_DEBUG << "Setting PostgreSQL connection to non-blocking mode";
    PQsetnonblocking(connectionPtr_.get(), 1);
    channel_.setReadCallback([this]() {
        if (status_ == ConnectStatus::Bad)
        {
            return;
        }
        if (status_ != ConnectStatus::Ok)
        {
            pgPoll();
        }
        else
        {
            handleRead();
        }
    });
    channel_.setWriteCallback([this]() {
        if (status_ == ConnectStatus::Ok)
        {
            LOG_TRACE << "Handling write event, flushing PostgreSQL connection";
            auto ret = PQflush(connectionPtr_.get());
            if (ret == 0)
            {
                LOG_TRACE << "PQflush completed, disabling writing";
                channel_.disableWriting();
                return;
            }
            else if (ret < 0)
            {
                channel_.disableWriting();
                LOG_ERROR << "PQflush error: "
                          << PQerrorMessage(connectionPtr_.get());
                return;
            }
        }
        else
        {
            LOG_TRACE << "Connection not ready, polling PostgreSQL connection";
            pgPoll();
        }
    });
    channel_.setCloseCallback([this]() {
        LOG_INFO << "Channel closed for PostgreSQL connection";
        handleClosed();
    });
    channel_.setErrorCallback([this]() {
        LOG_ERROR << "Channel error for PostgreSQL connection";
        handleClosed();
    });
    LOG_DEBUG << "Enabling reading and writing on PostgreSQL connection";
    channel_.enableReading();
    channel_.enableWriting();
}

void PgConnection::handleClosed()
{
    loop_->assertInLoopThread();
    LOG_INFO << "Handling closed PostgreSQL connection";
    if (status_ == ConnectStatus::Bad)
    {
        LOG_TRACE << "Connection already marked as Bad, ignoring close event";
        return;
    }
    status_ = ConnectStatus::Bad;

    if (isWorking_)
    {
        LOG_WARN << "PostgreSQL connection closed unexpectedly while "
                    "processing a query";
        isWorking_ = false;
        handleFatalError();
        callback_ = nullptr;
    }

    LOG_DEBUG
        << "Disabling and removing channel for closed PostgreSQL connection";
    channel_.disableAll();
    channel_.remove();
    assert(closeCallback_);
    auto thisPtr = shared_from_this();
    closeCallback_(thisPtr);
}

void PgConnection::disconnect()
{
    LOG_INFO << "Disconnecting from PostgreSQL server";
    std::promise<int> pro;
    auto f = pro.get_future();
    auto thisPtr = shared_from_this();
    loop_->runInLoop([thisPtr, &pro]() {
        LOG_DEBUG << "Executing disconnect in loop";
        thisPtr->status_ = ConnectStatus::Bad;
        if (thisPtr->channel_.fd() >= 0)
        {
            LOG_DEBUG << "Closing channel with fd: " << thisPtr->channel_.fd();
            thisPtr->channel_.disableAll();
            thisPtr->channel_.remove();
        }
        thisPtr->connectionPtr_.reset();
        LOG_DEBUG << "PostgreSQL connection resources released";
        pro.set_value(1);
    });
    f.get();
    LOG_INFO << "PostgreSQL disconnect completed";
}

void PgConnection::pgPoll()
{
    loop_->assertInLoopThread();
    LOG_TRACE << "Polling PostgreSQL connection";
    auto connStatus = PQconnectPoll(connectionPtr_.get());

    switch (connStatus)
    {
        case PGRES_POLLING_FAILED:
            LOG_ERROR << "PostgreSQL connection failed: "
                      << PQerrorMessage(connectionPtr_.get());
            if (status_ == ConnectStatus::None)
            {
                LOG_INFO << "Handling connection failure";
                handleClosed();
            }
            break;
        case PGRES_POLLING_WRITING:
            LOG_TRACE << "PostgreSQL connection needs write operation";
            if (!channel_.isWriting())
                channel_.enableWriting();
            break;
        case PGRES_POLLING_READING:
            LOG_TRACE << "PostgreSQL connection needs read operation";
            if (!channel_.isReading())
                channel_.enableReading();
            if (channel_.isWriting())
                channel_.disableWriting();
            break;

        case PGRES_POLLING_OK:
            LOG_DEBUG << "PostgreSQL connection established successfully";
            if (status_ != ConnectStatus::Ok)
            {
                status_ = ConnectStatus::Ok;
                assert(okCallback_);
                okCallback_(shared_from_this());
            }
            if (!channel_.isReading())
                channel_.enableReading();
            if (channel_.isWriting())
                channel_.disableWriting();
            break;
        case PGRES_POLLING_ACTIVE:
            LOG_TRACE << "PostgreSQL connection polling active (unused state)";
            break;
        default:
            LOG_WARN << "Unknown PostgreSQL polling status: " << connStatus;
            break;
    }
}

void PgConnection::execSqlInLoop(
    std::string_view &&sql,
    size_t paraNum,
    std::vector<const char *> &&parameters,
    std::vector<int> &&length,
    std::vector<int> &&format,
    ResultCallback &&rcb,
    std::function<void(const std::exception_ptr &)> &&exceptCallback)
{
    LOG_TRACE << "Executing SQL: " << sql;
    loop_->assertInLoopThread();
    assert(paraNum == parameters.size());
    assert(paraNum == length.size());
    assert(paraNum == format.size());
    assert(rcb);
    assert(!isWorking_);
    assert(!sql.empty());
    sql_ = std::move(sql);
    callback_ = std::move(rcb);
    isWorking_ = true;
    exceptionCallback_ = std::move(exceptCallback);
    if (paraNum == 0)
    {
        isPreparingStatement_ = false;
        if (PQsendQuery(connectionPtr_.get(), sql_.data()) == 0)
        {
            LOG_ERROR << "Failed to send query: "
                      << PQerrorMessage(connectionPtr_.get());
            if (isWorking_)
            {
                isWorking_ = false;
                isPreparingStatement_ = false;
                handleFatalError();
                callback_ = nullptr;
                idleCb_();
            }
            return;
        }
        flush();
    }
    else
    {
        auto iter = preparedStatementsMap_.find(sql_);
        if (iter != preparedStatementsMap_.end())
        {
            LOG_TRACE << "Executing prepared statement: " << iter->second;
            isPreparingStatement_ = false;
            if (PQsendQueryPrepared(connectionPtr_.get(),
                                    iter->second.c_str(),
                                    static_cast<int>(paraNum),
                                    parameters.data(),
                                    length.data(),
                                    format.data(),
                                    0) == 0)
            {
                LOG_ERROR << "Failed to send prepared query: "
                          << PQerrorMessage(connectionPtr_.get());
                if (isWorking_)
                {
                    isWorking_ = false;
                    isPreparingStatement_ = false;
                    handleFatalError();
                    callback_ = nullptr;
                    idleCb_();
                }
                return;
            }
        }
        else
        {
            isPreparingStatement_ = true;
            statementName_ = newStmtName();
            if (PQsendPrepare(connectionPtr_.get(),
                              statementName_.c_str(),
                              sql_.data(),
                              static_cast<int>(paraNum),
                              nullptr) == 0)
            {
                LOG_ERROR << "Failed to prepare statement: "
                          << PQerrorMessage(connectionPtr_.get());
                if (isWorking_)
                {
                    isWorking_ = false;
                    handleFatalError();
                    callback_ = nullptr;
                    idleCb_();
                }
                return;
            }
            parametersNumber_ = static_cast<int>(paraNum);
            parameters_ = std::move(parameters);
            lengths_ = std::move(length);
            formats_ = std::move(format);
        }
        flush();
    }
}

void PgConnection::handleRead()
{
    loop_->assertInLoopThread();
    std::shared_ptr<PGresult> res;

    if (!PQconsumeInput(connectionPtr_.get()))
    {
        LOG_ERROR << "Failed to consume PostgreSQL input: "
                  << PQerrorMessage(connectionPtr_.get());
        if (isWorking_)
        {
            isWorking_ = false;
            handleFatalError();
            callback_ = nullptr;
        }
        handleClosed();
        return;
    }
    if (PQisBusy(connectionPtr_.get()))
    {
        // need read more data from socket;
        return;
    }

    int resultCount = 0;
    while ((res = std::shared_ptr<PGresult>(PQgetResult(connectionPtr_.get()),
                                            [](PGresult *p) { PQclear(p); })))
    {
        resultCount++;
        auto type = PQresultStatus(res.get());
        LOG_TRACE << "Got PostgreSQL result #" << resultCount
                  << " with status: " << PQresStatus(type);

        if (type == PGRES_BAD_RESPONSE || type == PGRES_FATAL_ERROR)
        {
            LOG_WARN << "PostgreSQL error: "
                     << PQerrorMessage(connectionPtr_.get());
            if (isWorking_)
            {
                handleFatalError();
                callback_ = nullptr;
            }
        }
        else
        {
            if (isWorking_)
            {
                if (!isPreparingStatement_)
                {
                    LOG_DEBUG << "Processing query result";
                    auto r = makeResult(std::move(res));
                    callback_(r);
                    callback_ = nullptr;
                    exceptionCallback_ = nullptr;
                }
                else
                {
                    LOG_TRACE << "Prepared statement result received";
                }
            }
        }
    }

    LOG_TRACE << "Processed " << resultCount << " PostgreSQL results";

    if (isWorking_)
    {
        if (isPreparingStatement_ && callback_)
        {
            LOG_DEBUG << "Statement prepared, executing with parameters";
            doAfterPreparing();
        }
        else
        {
            LOG_DEBUG << "Query processing completed";
            isWorking_ = false;
            isPreparingStatement_ = false;
            idleCb_();
        }
    }

    // Check notification
    std::shared_ptr<PGnotify> notify;
    int notificationCount = 0;
    while (
        (notify = std::shared_ptr<PGnotify>(PQnotifies(connectionPtr_.get()),
                                            [](PGnotify *p) { PQfreemem(p); })))
    {
        notificationCount++;
        LOG_TRACE << "Received PostgreSQL notification #" << notificationCount
                  << " channel: " << notify->relname
                  << " payload: " << notify->extra;
        messageCallback_({notify->relname}, {notify->extra});
    }

    if (notificationCount > 0)
    {
        LOG_DEBUG << "Processed " << notificationCount
                  << " PostgreSQL notifications";
    }
}

void PgConnection::doAfterPreparing()
{
    isPreparingStatement_ = false;
    auto r = preparedStatements_.insert(std::string{sql_});
    preparedStatementsMap_[std::string_view{r.first->data(),
                                            r.first->length()}] =
        statementName_;
    if (PQsendQueryPrepared(connectionPtr_.get(),
                            statementName_.c_str(),
                            parametersNumber_,
                            parameters_.data(),
                            lengths_.data(),
                            formats_.data(),
                            0) == 0)
    {
        LOG_ERROR << "Failed to send prepared query: "
                  << PQerrorMessage(connectionPtr_.get());
        if (isWorking_)
        {
            isWorking_ = false;
            handleFatalError();
            callback_ = nullptr;
            idleCb_();
        }
        return;
    }
    flush();
}

void PgConnection::handleFatalError()
{
    if (exceptionCallback_)
    {
        auto exceptPtr = std::make_exception_ptr(
            Failure(PQerrorMessage(connectionPtr_.get())));
        exceptionCallback_(exceptPtr);
    }

    if (PQstatus(connectionPtr_.get()) != CONNECTION_OK)
    {
        LOG_ERROR << "Connection lost: "
                  << PQerrorMessage(connectionPtr_.get());

        handleClosed();
    }

    exceptionCallback_ = nullptr;
}

void PgConnection::batchSql(std::deque<std::shared_ptr<SqlCmd>> &&)
{
    assert(false);
}
