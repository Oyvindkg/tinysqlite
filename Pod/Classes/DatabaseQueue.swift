//
//  DatabaseQueue.swift
//  Ladestasjoner
//
//  Created by Øyvind Grimnes on 28/12/15.
//  Copyright © 2015 Øyvind Grimnes. All rights reserved.
//

import Foundation

// TODO: Allow queues working on different databases at the same time
private let _queue: dispatch_queue_t = dispatch_queue_create( ("swdb.\(arc4random())"), nil)

class DatabaseQueue {
    
    private let database:       DatabaseConnection
    
    init(path: String) {
        database = DatabaseConnection(path: path)
    }
    
    func transaction(block: ((database: DatabaseConnection) throws -> Void)) throws {
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
    
    func database(block: ((database: DatabaseConnection) throws -> Void)) throws {
        var thrownError: ErrorType?
        
        /* Run the query on a sequential queue to avoid threading related problems */
        dispatch_sync(_queue) { () -> Void in
            
            /* After trying to execute the block, close the database if it is open */
            defer {
                if self.database.isOpen {
                    try? self.database.close()
                }
            }
            
            /* Open the database and execute the block. Pass on any errors thrown */
            do {
                try self.database.open()
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