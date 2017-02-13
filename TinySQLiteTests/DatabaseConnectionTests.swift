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
    
    override func setUp() {
        super.setUp()
        
        let documentsDirectory = try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
        
        let databaseLocaiton = documentsDirectory.appendingPathComponent("database").appendingPathExtension("sqlite")
        
        try? FileManager.default.removeItem(at: databaseLocaiton)
        
        database = DatabaseConnection(location: databaseLocaiton)
        
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
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
        
        expect(try self.database.prepare(query: query)).to(throwError())
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
        
        try! database.prepare(query: "CREATE TABLE dog (name TEXT, age INTEGER)").executeUpdate()
        
        try! database.rollbackTransaction()
        
        expect(try self.database.containsTable("dog")) == false
    }
    
    func testTransactionIsCommitedOnEnd() {
        try! database.open()
        try! database.beginTransaction()
        
        try! database.prepare(query: "CREATE TABLE dog (name TEXT, age INTEGER)").executeUpdate()
        
        try! database.endTransaction()
        
        expect(try self.database.containsTable("dog")) == true
    }
    
    func testNumberOfRowsChangesInLastQueryIsZeroBeforeUpdates() {
        try! database.open()
        
        expect(self.database.numberOfRowsChangedInLastQuery()) == 0
    }
    
    func testTotalNumberOfRowsChangesIsZeroBeforeUpdates() {
        try! database.open()
        
        expect(self.database.totalNumberOfRowsChanged()) == 0
    }
    
    func testNumberOfRowsChangesInLastQueryIsNotZeroAfterAnUpdate() {
        try! database.open()
        
        try! database.prepare(query: "CREATE TABLE dog (name TEST, age INTEGER)").executeUpdate()
        try! database.prepare(query: "INSERT INTO dog VALUES (?, ?)").executeUpdate(values: ["fido", 3])
        
        expect(self.database.numberOfRowsChangedInLastQuery()) == 1
    }
    
    func testTotalNumberOfRowsChangedAccumulatesTheNumberOfRowChanges() {
        try! database.open()
        
        try! database.prepare(query: "CREATE TABLE dog (name TEST, age INTEGER)").executeUpdate()
        try! database.prepare(query: "INSERT INTO dog VALUES (?, ?)").executeUpdate(values: ["fido", 3])
        try! database.prepare(query: "INSERT INTO dog VALUES (?, ?)").executeUpdate(values: ["fido", 3])
        
        expect(self.database.totalNumberOfRowsChanged()) == 2
    }
    
    func testNumberOfRowsChangesInLastQueryDoesNotAccumulateTheNumberOfRowChanges() {
        try! database.open()
        
        try! database.prepare(query: "CREATE TABLE dog (name TEST, age INTEGER)").executeUpdate()
        try! database.prepare(query: "INSERT INTO dog VALUES (?, ?)").executeUpdate(values: ["fido", 3])
        try! database.prepare(query: "INSERT INTO dog VALUES (?, ?)").executeUpdate(values: ["fido", 3])
        
        expect(self.database.numberOfRowsChangedInLastQuery()) == 1
    }
    
    func testContainsTableReturnsFalseIfTableDoesNotExist() {
        try! database.open()
        
        expect(try! self.database.containsTable("dog")) == false
    }
    
    func testContainsTableReturnsTrueIfTableExists() {
        try! database.open()
        
        try! database.prepare(query: "CREATE TABLE dog (name TEST, age INTEGER)").executeUpdate()
        
        expect(try! self.database.containsTable("dog")) == true
    }
}
