// narkdb_util.cpp

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "narkdb_util.h"

#include <limits>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "narkdb_recovery_unit.h"
#include "narkdb_session_cache.h"
#include "mongo/platform/unordered_set.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"

namespace mongo { namespace narkdb {

using std::string;

Status narkDbRCToStatus_slow(int retCode, const char* prefix) {
    if (retCode == 0)
        return Status::OK();

    if (retCode == NarkDb_ROLLBACK) {
        throw WriteConflictException();
    }

    fassert(28559, retCode != NarkDb_PANIC);

    str::stream s;
    if (prefix)
        s << prefix << " ";
    s << retCode << ": " << narkdb_strerror(retCode);

    if (retCode == EINVAL) {
        return Status(ErrorCodes::BadValue, s);
    }

    // TODO convert specific codes rather than just using UNKNOWN_ERROR for everything.
    return Status(ErrorCodes::UnknownError, s);
}

void NarkDbUtil::fetchTypeAndSourceURI(OperationContext* opCtx,
                                           const std::string& tableUri,
                                           std::string* type,
                                           std::string* source) {
    std::string colgroupUri = "colgroup";
    const size_t colon = tableUri.find(':');
    invariant(colon != string::npos);
    colgroupUri += tableUri.substr(colon);
    StatusWith<std::string> colgroupResult = getMetadata(opCtx, colgroupUri);
    invariant(colgroupResult.isOK());
    NarkDbConfigParser parser(colgroupResult.getValue());

    NarkDb_CONFIG_ITEM typeItem;
    invariant(parser.get("type", &typeItem) == 0);
    invariant(typeItem.type == NarkDb_CONFIG_ITEM::NarkDb_CONFIG_ITEM_ID);
    *type = std::string(typeItem.str, typeItem.len);

    NarkDb_CONFIG_ITEM sourceItem;
    invariant(parser.get("source", &sourceItem) == 0);
    invariant(sourceItem.type == NarkDb_CONFIG_ITEM::NarkDb_CONFIG_ITEM_STRING);
    *source = std::string(sourceItem.str, sourceItem.len);
}

StatusWith<std::string> NarkDbUtil::getMetadata(OperationContext* opCtx, StringData uri) {
    invariant(opCtx);
    NarkDbCursor curwrap("metadata:create", NarkDbSession::kMetadataTableId, false, opCtx);
    NarkDb_CURSOR* cursor = curwrap.get();
    invariant(cursor);
    std::string strUri = uri.toString();
    cursor->set_key(cursor, strUri.c_str());
    int ret = cursor->search(cursor);
    if (ret == NarkDb_NOTFOUND) {
        return StatusWith<std::string>(ErrorCodes::NoSuchKey,
                                       str::stream() << "Unable to find metadata for " << uri);
    } else if (ret != 0) {
        return StatusWith<std::string>(narkDbRCToStatus(ret));
    }
    const char* metadata = NULL;
    ret = cursor->get_value(cursor, &metadata);
    if (ret != 0) {
        return StatusWith<std::string>(narkDbRCToStatus(ret));
    }
    invariant(metadata);
    return StatusWith<std::string>(metadata);
}

Status NarkDbUtil::getApplicationMetadata(OperationContext* opCtx,
                                              StringData uri,
                                              BSONObjBuilder* bob) {
    StatusWith<std::string> metadataResult = getMetadata(opCtx, uri);
    if (!metadataResult.isOK()) {
        return metadataResult.getStatus();
    }
    NarkDbConfigParser topParser(metadataResult.getValue());
    NarkDb_CONFIG_ITEM appMetadata;
    if (topParser.get("app_metadata", &appMetadata) != 0) {
        return Status::OK();
    }
    if (appMetadata.len == 0) {
        return Status::OK();
    }
    if (appMetadata.type != NarkDb_CONFIG_ITEM::NarkDb_CONFIG_ITEM_STRUCT) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "app_metadata must be a nested struct. Actual value: "
                                    << StringData(appMetadata.str, appMetadata.len));
    }

    NarkDbConfigParser parser(appMetadata);
    NarkDb_CONFIG_ITEM keyItem;
    NarkDb_CONFIG_ITEM valueItem;
    int ret;
    unordered_set<StringData, StringData::Hasher> keysSeen;
    while ((ret = parser.next(&keyItem, &valueItem)) == 0) {
        const StringData key(keyItem.str, keyItem.len);
        if (keysSeen.count(key)) {
            return Status(ErrorCodes::DuplicateKey,
                          str::stream() << "app_metadata must not contain duplicate keys. "
                                        << "Found multiple instances of key '" << key << "'.");
        }
        keysSeen.insert(key);

        switch (valueItem.type) {
            case NarkDb_CONFIG_ITEM::NarkDb_CONFIG_ITEM_BOOL:
                bob->appendBool(key, valueItem.val);
                break;
            case NarkDb_CONFIG_ITEM::NarkDb_CONFIG_ITEM_NUM:
                bob->appendIntOrLL(key, valueItem.val);
                break;
            default:
                bob->append(key, StringData(valueItem.str, valueItem.len));
                break;
        }
    }
    if (ret != NarkDb_NOTFOUND) {
        return narkDbRCToStatus(ret);
    }

    return Status::OK();
}

StatusWith<BSONObj> NarkDbUtil::getApplicationMetadata(OperationContext* opCtx,
                                                           StringData uri) {
    BSONObjBuilder bob;
    Status status = getApplicationMetadata(opCtx, uri, &bob);
    if (!status.isOK()) {
        return StatusWith<BSONObj>(status);
    }
    return StatusWith<BSONObj>(bob.obj());
}

Status NarkDbUtil::checkApplicationMetadataFormatVersion(OperationContext* opCtx,
                                                             StringData uri,
                                                             int64_t minimumVersion,
                                                             int64_t maximumVersion) {
    StatusWith<std::string> result = getMetadata(opCtx, uri);
    if (result.getStatus().code() == ErrorCodes::NoSuchKey) {
        return result.getStatus();
    }
    invariantOK(result.getStatus());

    NarkDbConfigParser topParser(result.getValue());
    NarkDb_CONFIG_ITEM metadata;
    if (topParser.get("app_metadata", &metadata) != 0)
        return Status(ErrorCodes::UnsupportedFormat,
                      str::stream() << "application metadata for " << uri << " is missing ");

    NarkDbConfigParser parser(metadata);

    int64_t version = 0;
    NarkDb_CONFIG_ITEM versionItem;
    if (parser.get("formatVersion", &versionItem) != 0) {
        // If 'formatVersion' is missing, this metadata was introduced by
        // one of the RC versions (where the format version is 1).
        version = 1;
    } else if (versionItem.type == NarkDb_CONFIG_ITEM::NarkDb_CONFIG_ITEM_NUM) {
        version = versionItem.val;
    } else {
        return Status(ErrorCodes::UnsupportedFormat,
                      str::stream() << "'formatVersion' in application metadata for " << uri
                                    << " must be a number. Current value: "
                                    << StringData(versionItem.str, versionItem.len));
    }

    if (version < minimumVersion || version > maximumVersion) {
        return Status(ErrorCodes::UnsupportedFormat,
                      str::stream() << "Application metadata for " << uri
                                    << " has unsupported format version " << version);
    }

    LOG(2) << "NarkDbUtil::checkApplicationMetadataFormatVersion "
           << " uri: " << uri << " ok range " << minimumVersion << " -> " << maximumVersion
           << " current: " << version;

    return Status::OK();
}

// static
Status NarkDbUtil::checkTableCreationOptions(const BSONElement& configElem) {
    invariant(configElem.fieldNameStringData() == "configString");

    if (configElem.type() != String) {
        return {ErrorCodes::TypeMismatch, "'configString' must be a string."};
    }

    std::vector<std::string> errors;
    ErrorAccumulator eventHandler(&errors);

    StringData config = configElem.valueStringData();
    Status status = narkDbRCToStatus(
        narkdb_config_validate(nullptr, &eventHandler, "NarkDb_SESSION.create", config.rawData()));
    if (!status.isOK()) {
        StringBuilder errorMsg;
        errorMsg << status.reason();
        for (std::string error : errors) {
            errorMsg << ". " << error;
        }
        errorMsg << ".";
        return {status.code(), errorMsg.str()};
    }
    return Status::OK();
}

// static
StatusWith<uint64_t> NarkDbUtil::getStatisticsValue(NarkDb_SESSION* session,
                                                        const std::string& uri,
                                                        const std::string& config,
                                                        int statisticsKey) {
    invariant(session);
    NarkDb_CURSOR* cursor = NULL;
    const char* cursorConfig = config.empty() ? NULL : config.c_str();
    int ret = session->open_cursor(session, uri.c_str(), NULL, cursorConfig, &cursor);
    if (ret != 0) {
        return StatusWith<uint64_t>(ErrorCodes::CursorNotFound,
                                    str::stream() << "unable to open cursor at URI " << uri
                                                  << ". reason: " << narkdb_strerror(ret));
    }
    invariant(cursor);
    ON_BLOCK_EXIT(cursor->close, cursor);

    cursor->set_key(cursor, statisticsKey);
    ret = cursor->search(cursor);
    if (ret != 0) {
        return StatusWith<uint64_t>(ErrorCodes::NoSuchKey,
                                    str::stream() << "unable to find key " << statisticsKey
                                                  << " at URI " << uri
                                                  << ". reason: " << narkdb_strerror(ret));
    }

    uint64_t value;
    ret = cursor->get_value(cursor, NULL, NULL, &value);
    if (ret != 0) {
        return StatusWith<uint64_t>(ErrorCodes::BadValue,
                                    str::stream() << "unable to get value for key " << statisticsKey
                                                  << " at URI " << uri
                                                  << ". reason: " << narkdb_strerror(ret));
    }

    return StatusWith<uint64_t>(value);
}

int64_t NarkDbUtil::getIdentSize(NarkDb_SESSION* s, const std::string& uri) {
    StatusWith<int64_t> result = NarkDbUtil::getStatisticsValueAs<int64_t>(
        s, "statistics:" + uri, "statistics=(size)", NarkDb_STAT_DSRC_BLOCK_SIZE);
    const Status& status = result.getStatus();
    if (!status.isOK()) {
        if (status.code() == ErrorCodes::CursorNotFound) {
            // ident gone, so its 0
            return 0;
        }
        uassertStatusOK(status);
    }
    return result.getValue();
}

namespace {
int mdb_handle_error(NarkDb_EVENT_HANDLER* handler,
                     NarkDb_SESSION* session,
                     int errorCode,
                     const char* message) {
    try {
        error() << "NarkDb (" << errorCode << ") " << message;
        fassert(28558, errorCode != NarkDb_PANIC);
    } catch (...) {
        std::terminate();
    }
    return 0;
}

int mdb_handle_message(NarkDb_EVENT_HANDLER* handler, NarkDb_SESSION* session, const char* message) {
    try {
        log() << "NarkDb " << message;
    } catch (...) {
        std::terminate();
    }
    return 0;
}

int mdb_handle_progress(NarkDb_EVENT_HANDLER* handler,
                        NarkDb_SESSION* session,
                        const char* operation,
                        uint64_t progress) {
    try {
        log() << "NarkDb progress " << operation << " " << progress;
    } catch (...) {
        std::terminate();
    }

    return 0;
}
}

NarkDb_EVENT_HANDLER NarkDbUtil::defaultEventHandlers() {
    NarkDb_EVENT_HANDLER handlers = {};
    handlers.handle_error = mdb_handle_error;
    handlers.handle_message = mdb_handle_message;
    handlers.handle_progress = mdb_handle_progress;
    return handlers;
}

NarkDbUtil::ErrorAccumulator::ErrorAccumulator(std::vector<std::string>* errors)
    : NarkDb_EVENT_HANDLER(defaultEventHandlers()),
      _errors(errors),
      _defaultErrorHandler(handle_error) {
    if (errors) {
        handle_error = onError;
    }
}

// static
int NarkDbUtil::ErrorAccumulator::onError(NarkDb_EVENT_HANDLER* handler,
                                              NarkDb_SESSION* session,
                                              int error,
                                              const char* message) {
    try {
        ErrorAccumulator* self = static_cast<ErrorAccumulator*>(handler);
        self->_errors->push_back(message);
        return self->_defaultErrorHandler(handler, session, error, message);
    } catch (...) {
        std::terminate();
    }
}

int NarkDbUtil::verifyTable(OperationContext* txn,
                                const std::string& uri,
                                std::vector<std::string>* errors) {
    ErrorAccumulator eventHandler(errors);

    // Try to close as much as possible to avoid EBUSY errors.
    NarkDbRecoveryUnit::get(txn)->getSession(txn)->closeAllCursors();
    NarkDbSessionCache* sessionCache = NarkDbRecoveryUnit::get(txn)->getSessionCache();
    sessionCache->closeAll();

    // Open a new session with custom error handlers.
    NarkDb_CONNECTION* conn = NarkDbRecoveryUnit::get(txn)->getSessionCache()->conn();
    NarkDb_SESSION* session;
    invariantNarkDbOK(conn->open_session(conn, &eventHandler, NULL, &session));
    ON_BLOCK_EXIT(session->close, session, "");

    // Do the verify. Weird parents prevent treating "verify" as a macro.
    return (session->verify)(session, uri.c_str(), NULL);
}

Status NarkDbUtil::exportTableToBSON(NarkDb_SESSION* session,
                                         const std::string& uri,
                                         const std::string& config,
                                         BSONObjBuilder* bob) {
    invariant(session);
    invariant(bob);
    NarkDb_CURSOR* c = NULL;
    const char* cursorConfig = config.empty() ? NULL : config.c_str();
    int ret = session->open_cursor(session, uri.c_str(), NULL, cursorConfig, &c);
    if (ret != 0) {
        return Status(ErrorCodes::CursorNotFound,
                      str::stream() << "unable to open cursor at URI " << uri
                                    << ". reason: " << narkdb_strerror(ret));
    }
    bob->append("uri", uri);
    invariant(c);
    ON_BLOCK_EXIT(c->close, c);

    std::map<string, BSONObjBuilder*> subs;
    const char* desc;
    uint64_t value;
    while (c->next(c) == 0 && c->get_value(c, &desc, NULL, &value) == 0) {
        StringData key(desc);

        StringData prefix;
        StringData suffix;

        size_t idx = key.find(':');
        if (idx != string::npos) {
            prefix = key.substr(0, idx);
            suffix = key.substr(idx + 1);
        } else {
            idx = key.find(' ');
        }

        if (idx != string::npos) {
            prefix = key.substr(0, idx);
            suffix = key.substr(idx + 1);
        } else {
            prefix = key;
            suffix = "num";
        }

        long long v = _castStatisticsValue<long long>(value);

        if (prefix.size() == 0) {
            bob->appendNumber(desc, v);
        } else {
            BSONObjBuilder*& sub = subs[prefix.toString()];
            if (!sub)
                sub = new BSONObjBuilder();
            sub->appendNumber(mongoutils::str::ltrim(suffix.toString()), v);
        }
    }

    for (std::map<string, BSONObjBuilder*>::const_iterator it = subs.begin(); it != subs.end();
         ++it) {
        const std::string& s = it->first;
        bob->append(s, it->second->obj());
        delete it->second;
    }
    return Status::OK();
}

} } // namespace mongo::narkdb
