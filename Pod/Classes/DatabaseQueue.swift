//
//  DatabaseQueue.swift
//  TinySQLite
//
//  Created by Øyvind Grimnes on 28/12/15.
//

import Foundation

// TODO: Allow queues working on different databases at the same time
private let _queue: dispatch_queue_t = dispatch_queue_create( ("swdb.\(arc4random())"), nil)

public class DatabaseQueue {
    
    private let database:       DatabaseConnection
    
    public init(path: String) {
        database = DatabaseConnection(path: path)
    }
    
    public func transaction(block: ((database: DatabaseConnection) throws -> Void)) throws {
        try database { (database) -> Void in
            /* If an error occurs, rollback the transaction and rethrow the error */
            do {
                try database.beginTransaction()
                try block(database: database)
                try database.endTransaction()
            } catch let error {
                try? database.rollback()
                throw error
            }
        }
    }
    
    public func database(block: ((database: DatabaseConnection) throws -> Void)) throws {
        var thrownError: ErrorType?
        
        /* Run the query on a sequential queue to avoid threading related problems */
        dispatch_sync(_queue) { () -> Void in
            
            /* Open the database and execute the block. Pass on any errors thrown */
            do {
                try self.database.open()
                
                /* Close the database when leaving this scope */
                defer {
                    try? self.database.close()
                }
                
                try block(database: self.database)
            } catch let error {
                thrownError = error
            }
        }
        
        /* If an error was thrown during execution, rethrow it */
        // TODO: Improve the process of passing along the error
        guard thrownError == nil else {
            throw thrownError!
        }
    }
}