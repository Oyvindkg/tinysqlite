//
//  DatabaseConnection.swift
//  TinySQLite
//
//  Created by Øyvind Grimnes on 25/12/15.
//

import sqlite3

// MARK: - Setup SQLiteValue protocol for all supported  datatypes

/** Valid SQLite types are marked using the 'SQLiteValue' protocol */
public protocol SQLiteValue {}

extension String: SQLiteValue {}
extension NSString: SQLiteValue {}
extension Character: SQLiteValue {}

extension Bool: SQLiteValue {}

extension Int: SQLiteValue {}
extension Int8: SQLiteValue {}
extension Int16: SQLiteValue {}
extension Int32: SQLiteValue {}
extension Int64: SQLiteValue {}
extension UInt: SQLiteValue {}
extension UInt8: SQLiteValue {}
extension UInt16: SQLiteValue {}
extension UInt32: SQLiteValue {}
extension UInt64: SQLiteValue {}

extension Float: SQLiteValue {}
extension Float80: SQLiteValue {}
extension Double: SQLiteValue {}

extension NSData: SQLiteValue {}
extension NSDate: SQLiteValue {}
extension NSNumber: SQLiteValue {}

public typealias SQLiteValues = Array<SQLiteValue?>
public typealias NamedSQLiteValues = Dictionary<String, SQLiteValue?>

// MARK: -


public struct SQLiteResultHandler {
    static let successCodes: Set<Int32> = [SQLITE_OK, SQLITE_DONE, SQLITE_ROW]
    
    static func isSuccess(resultCode: Int32) -> Bool {
        return SQLiteResultHandler.successCodes.contains(resultCode)
    }
    
    static func verifyResultCode(resultCode: Int32, forHandle handle: COpaquePointer) throws {
        guard isSuccess(resultCode) else {
            let errorMessage = NSString(UTF8String: sqlite3_errmsg(handle)) as? String
            
            throw TinySQLiteError.SQLiteError(message: "\(errorMessage ?? "ERROR"): " + SQLiteResultHandler.resultMessageForResultCode(resultCode) + " (\(resultCode))")
        }
    }
    
    static func resultMessageForResultCode(resultCode: Int32) -> String {
        switch resultCode {
        case SQLITE_OK:
            return "Successful result"
        case SQLITE_ERROR:
            return "SQL error or missing database"
        case SQLITE_BUSY:
            return "The database file is locked"
        case SQLITE_CONSTRAINT:
            return "Abort due to constraint violation"
        case SQLITE_MISMATCH:
            return "Data type mismatch"
        case SQLITE_MISUSE:
            return "Library used incorrectly"
        case SQLITE_ROW:
            return "sqlite3_step() has another row ready"
        case SQLITE_DONE:
            return "sqlite3_step() has finished executing"
        default:
            return "No message configured for result code \(resultCode)"
        }
    }
}

// MARK: -

public enum TinySQLiteError: ErrorType {
    case SQLiteError(message: String)
    case UnknownBindingType(message: String)
    case WrongNumberOfBindings(message: String)
}

internal let SQLITE_STATIC = unsafeBitCast(0, sqlite3_destructor_type.self)
internal let SQLITE_TRANSIENT = unsafeBitCast(-1, sqlite3_destructor_type.self)


/** Responsible for opening and closing database connections, executing queries, and managing transactions */
public class DatabaseConnection {
    
    private var handle: COpaquePointer = nil
    private let path: String
    
    public var isOpen: Bool = false
    
    public init(path: String) {
        self.path = path
    }
    
    public func open() throws {
        try SQLiteResultHandler.verifyResultCode(sqlite3_open(path, &handle), forHandle: handle)
        isOpen = true
    }
    
    public func close() throws {
        try SQLiteResultHandler.verifyResultCode(sqlite3_close(handle), forHandle: handle)
        handle = nil
        isOpen = false
    }
    
    public func executeUpdate(query: String, values: SQLiteValues = []) throws {
        try executeQuery(query, values: values).step()
    }
    
    public func executeUpdate(query: String, namedValues: NamedSQLiteValues) throws {
        try executeQuery(query, namedValues: namedValues).step()
    }
    
    public func executeQuery(query: String, values: SQLiteValues = []) throws -> Statement {
        let statement: Statement = Statement(query)
        try statement.prepareForDatabase(handle)
        try statement.bind(values)
        return statement
    }
    
    public func executeQuery(query: String, namedValues: NamedSQLiteValues) throws -> Statement {
        let statement: Statement = Statement(query)
        try statement.prepareForDatabase(handle)
        try statement.bind(namedValues)
        return statement
    }
}

// MARK: - Transactions
extension DatabaseConnection {
    func beginTransaction() throws {
        try self.executeUpdate("BEGIN TRANSACTION")
    }
    
    func endTransaction() throws {
        try self.executeUpdate("END TRANSACTION")
    }
    
    func rollback() throws {
        try self.executeUpdate("ROLLBACK TRANSACTION")
    }
}

// MARK: - General 
extension DatabaseConnection {
    
    /** Number of rows affected by INSERT, UPDATE, or DELETE since the database was opened */
    public func changes() -> Int {
        return Int(sqlite3_changes(handle))
    }
    
    /** Total number of rows affected by INSERT, UPDATE, or DELETE since the database was opened */
    public func totalChanges() -> Int {
        return Int(sqlite3_total_changes(handle))
    }
    
    /** Interrupts any pending database operations */
    public func interrupt() {
        sqlite3_interrupt(handle)
    }
}

// MARK: - Helpers
extension DatabaseConnection {
    public func containsTable(tableName: String) throws -> Bool {
        let query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        
        let statement = try executeQuery(query, values: [tableName])
        
        /* Finalize the statement if necessary */
        defer {
            if statement.isBusy {
                try! statement.finalize()
            }
        }
        
        return statement.next() != nil
    }
}