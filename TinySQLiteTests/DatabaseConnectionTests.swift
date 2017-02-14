//
//  DatabaseConnectionTests.swift
//  TinySQLiteTests
//
//  Created by Øyvind Grimnes on 13/02/17.
//  Copyright © 2017 Øyvind Grimnes. All rights reserved.
//

import XCTest
import Nimble

@testable import TinySQLite

class DatabaseConnectionTests: XCTestCase {
    
    var database: DatabaseConnection!
    
    var databaseLocation: URL {
        let documentsDirectory = try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
        
        return documentsDirectory.appendingPathComponent("database").appendingPathExtension("sqlite")
    }
    
    override func setUp() {
        super.setUp()
        
        try? FileManager.default.removeItem(at: databaseLocation)
        
        database = DatabaseConnection(location: databaseLocation)
    }
    
    override func tearDown() {
        
        try? FileManager.default.removeItem(at: databaseLocation)
        
        super.tearDown()
    }
    
    func testDatabaseIsClosedByDefault() {
        expect(self.database.isOpen) == false
    }
    
    func testDatabaseIsClosedAfterClose() {
        try! database.open()
        try! database.close()
        
        expect(self.database.isOpen) == false
    }
    
    func testThrowsNoErrorIfClosedOnClose() {
        expect(try self.database.close()).notTo(throwError())
    }
    
    func testThrowsNoErrorIfOpenOnOpen() {
        try! database.open()
        
        expect(try self.database.open()).notTo(throwError())
    }
    
    func testDatabaseIsOpenAfterOpen() {
        try! database.open()
        
        expect(self.database.isOpen) == true
    }
    
    func testThrowsErrorIfDatabaseIsNotOpenOnPrepare() {
        let query = "CREATE TABLE dog (name TEST, age INTEGER)"
        
        expect(try self.database.statement(for: query)).to(throwError())
    }
    
    func testThrowsErrorIfDatabaseIsNotOpenOnBeginTransaction() {
        expect(try self.database.beginTransaction()).to(throwError())
    }
    
    func testThrowsErrorIfTransactionIsEndedWhenNoTransactionIsActive() {
        try! database.open()
        
        expect(try self.database.endTransaction()).to(throwError())
    }
    
    func testThrowsErrorNoIfTransactionIsEndedWhenATransactionIsActive() {
        try! database.open()
        try! database.beginTransaction()
        
        expect(try self.database.endTransaction()).notTo(throwError())
    }
    
    func testThrowsErrorIfBeginTransactionIsCallenDuringATransaction() {
        try! database.open()
        try! database.beginTransaction()
        
        expect(try self.database.beginTransaction()).to(throwError())
    }
    
    func testThrowsNoErrorIfDatabaseIsOpenOnBeginTransaction() {
        try! database.open()
        
        expect(try self.database.beginTransaction()).notTo(throwError())
    }
    
    func testTransactionIsNotCommittedIfRollbackIsCalled() {
        try! database.open()
        try! database.beginTransaction()
        
        try! database.statement(for: "CREATE TABLE dog (name TEXT, age INTEGER)").executeUpdate()
        
        try! database.rollbackTransaction()
        
        expect(try self.database.contains(table: "dog")) == false
    }
    
    func testTransactionIsCommitedOnEnd() {
        try! database.open()
        try! database.beginTransaction()
        
        try! database.statement(for: "CREATE TABLE dog (name TEXT, age INTEGER)").executeUpdate()
        
        try! database.endTransaction()
        
        expect(try self.database.contains(table: "dog")) == true
    }
    
    func testNumberOfRowsChangesInLastQueryIsZeroBeforeUpdates() {
        try! database.open()
        
        expect(self.database.numberOfChanges) == 0
    }
    
    func testTotalNumberOfRowsChangesIsZeroBeforeUpdates() {
        try! database.open()
        
        expect(self.database.totalNumberOfChanges) == 0
    }
    
    func testNumberOfRowsChangesInLastQueryIsNotZeroAfterAnUpdate() {
        try! database.open()
        
        try! database.statement(for: "CREATE TABLE dog (name TEST, age INTEGER)").executeUpdate()
        try! database.statement(for: "INSERT INTO dog VALUES (?, ?)").executeUpdate(withParameters: ["fido", 3])
        
        expect(self.database.numberOfChanges) == 1
    }
    
    func testTotalNumberOfRowsChangedAccumulatesTheNumberOfRowChanges() {
        try! database.open()
        
        try! database.statement(for: "CREATE TABLE dog (name TEST, age INTEGER)").executeUpdate()
        try! database.statement(for: "INSERT INTO dog VALUES (?, ?)").executeUpdate(withParameters: ["fido", 3])
        try! database.statement(for: "INSERT INTO dog VALUES (?, ?)").executeUpdate(withParameters: ["fido", 3])
        
        expect(self.database.totalNumberOfChanges) == 2
    }
    
    func testNumberOfRowsChangesInLastQueryDoesNotAccumulateTheNumberOfRowChanges() {
        try! database.open()
        
        try! database.statement(for: "CREATE TABLE dog (name TEST, age INTEGER)").executeUpdate()
        try! database.statement(for: "INSERT INTO dog VALUES (?, ?)").executeUpdate(withParameters: ["fido", 3])
        try! database.statement(for: "INSERT INTO dog VALUES (?, ?)").executeUpdate(withParameters: ["fido", 3])
        
        expect(self.database.numberOfChanges) == 1
    }
    
    func testContainsTableReturnsFalseIfTableDoesNotExist() {
        try! database.open()
        
        expect(try! self.database.contains(table: "dog")) == false
    }
    
    func testContainsTableReturnsTrueIfTableExists() {
        try! database.open()
        
        try! database.statement(for: "CREATE TABLE dog (name TEST, age INTEGER)").executeUpdate()

        expect(try! self.database.contains(table: "dog")) == true
    }
    
    func testContainsIndexReturnsFalseIfIndexDoesNotExist() {
        try! database.open()
        
        expect(try! self.database.contains(index: "dognames")) == false
    }
    
    func testContainsIndexReturnsTrueIfIndexExists() {
        try! database.open()
        
        try! database.statement(for: "CREATE TABLE dog (name TEST, age INTEGER)").executeUpdate()
        try! database.statement(for: "CREATE INDEX dognames ON dog (name)").executeUpdate()
        
        expect(try! self.database.contains(index: "dognames")) == true
    }
    
    func testContainsViewReturnsFalseIfViewDoesNotExist() {
        try! database.open()
        
        expect(try! self.database.contains(view: "dogview")) == false
    }
    
    func testContainsViewReturnsTrueIfViewExists() {
        try! database.open()
        
        try! database.statement(for: "CREATE TABLE dog (name TEST, age INTEGER)").executeUpdate()
        try! database.statement(for: "CREATE VIEW dogview (name) AS SELECT name FROM dog WHERE name='fido'").executeUpdate()
        
        expect(try! self.database.contains(view: "dogview")) == true
    }
}
