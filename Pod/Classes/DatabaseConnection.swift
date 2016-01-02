//
//  DatabaseConnection.swift
//  Ladestasjoner
//
//  Created by Øyvind Grimnes on 25/12/15.
//  Copyright © 2015 Øyvind Grimnes. All rights reserved.
//

import sqlite3

/** Valid SQLite types are marked using the 'Binding' protocol */
public protocol Binding {}

extension String: Binding {}
extension Bool: Binding {}
extension Int: Binding {}
extension Float: Binding {}
extension Double: Binding {}
extension NSString: Binding {}
extension NSData: Binding {}
extension NSDate: Binding {}
extension NSNumber: Binding {}

public typealias Bindings = Array<Binding?>
public typealias NamedBindings = Dictionary<String, Binding?>


internal let SQLITE_STATIC = unsafeBitCast(0, sqlite3_destructor_type.self)
internal let SQLITE_TRANSIENT = unsafeBitCast(-1, sqlite3_destructor_type.self)


public struct SQLiteResultHandler {
    static let successCodes: Set<Int32> = [SQLITE_OK, SQLITE_DONE, SQLITE_ROW]
    
    static func isSuccess(resultCode: Int32) -> Bool {
        return SQLiteResultHandler.successCodes.contains(resultCode)
    }
    
    static func verifyResultCode(resultCode: Int32, forHandle handle: COpaquePointer) throws {
        guard isSuccess(resultCode) else {
            let errorMessage = NSString(UTF8String: sqlite3_errmsg(handle)) as? String
            
            throw DatabaseError.SQLite(message: "\(errorMessage ?? "ERROR"): " + SQLiteResultHandler.resultMessageForResultCode(resultCode) + " (\(resultCode))")
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

public enum DatabaseError: ErrorType {
    case SQLite(message: String)
    case Binding(message: String)
}



public class DatabaseConnection {
    
    private var handle: COpaquePointer = nil
    private let path: String
    
    var isOpen: Bool = false
    
    init(path: String) {
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
    
    
    public func executeUpdate(query: String, bindings: Bindings = []) throws {
        try executeQuery(query, bindings: bindings).step()
    }
    
    public func executeUpdate(query: String, namedBindings: NamedBindings) throws {
        try executeQuery(query, namedBindings: namedBindings).step()
    }
    
    public func executeQuery(query: String, bindings: Bindings = []) throws -> Statement {
        print(query)
        let statement: Statement = Statement(query)
        try statement.prepareForDatabase(handle)
        try statement.bind(bindings)
        return statement
    }
    
    public func executeQuery(query: String, namedBindings: NamedBindings) throws -> Statement {
        let statement: Statement = Statement(query)
        try statement.prepareForDatabase(handle)
        try statement.bind(namedBindings)
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


// MARK: - Helpers
extension DatabaseConnection {
    func containsTable(tableName: String) throws -> Bool {
        let query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        
        let statement = try executeQuery(query, bindings: [tableName])
        
        defer {
            if statement.isBusy {
                try! statement.finalize()
            }
        }
        
        return statement.next() != nil
    }
}