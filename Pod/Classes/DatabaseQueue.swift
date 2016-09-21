//
//  DatabaseQueue.swift
//  TinySQLite
//
//  Created by Øyvind Grimnes on 28/12/15.
//

import Foundation

// TODO: Allow queues working on different databases at the same time
private let _queue: DispatchQueue = DispatchQueue(label: "TinySQLiteQueue", attributes: [])

open class DatabaseQueue {
    
    fileprivate let database:       DatabaseConnection
    
    /** Create a database queue for the database at the provided path */
    public init(path: String) {
        database = DatabaseConnection(path: path)
    }
    
    /** Execute a synchronous transaction on the database in a sequential queue */
    open func transaction(_ block: ((_ database: DatabaseConnection) throws -> Void)) throws {
        try database { (database) -> Void in
            /* If an error occurs, rollback the transaction and rethrow the error */
            do {
                try database.beginTransaction()
                try block(database)
                try database.endTransaction()
            } catch let error {
                try database.rollback()
                throw error
            }
        }
    }
    
    /** Execute synchronous queries on the database in a sequential queue */
    open func database(_ block: ((_ database: DatabaseConnection) throws -> Void)) throws {
        var thrownError: Error?
        
        /* Run the query in a sequential queue to avoid threading related problems */
        _queue.sync { () -> Void in
            
            /* Open the database and execute the block. Pass on any errors thrown */
            do {
                try self.database.open()
                
                /* Close the database when leaving this scope */
                defer {
                    try! self.database.close()
                }
                
                try block(self.database)
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
