import UIKit
import XCTest
import TinySQLite

class DatabaseConnectionTests: XCTestCase {
    
    var path: String {
        let documentsDirectory : String = NSSearchPathForDirectoriesInDomains(NSSearchPathDirectory.DocumentDirectory, NSSearchPathDomainMask.UserDomainMask, true)[0]
        return documentsDirectory+"/testDatabase.sqlite"
    }
    var database: DatabaseConnection = DatabaseConnection(path: "")
    
    override func setUp() {
        super.setUp()
        
        database = DatabaseConnection(path: path)
        
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    override func tearDown() {
        try? NSFileManager.defaultManager().removeItemAtPath(path)
        
        super.tearDown()
    }
    
    func testDatabaseIsOpened() {
        XCTAssertFalse(database.isOpen)
        XCTAssertNotNil(try? database.open())
        XCTAssertTrue(database.isOpen)
    }
    
    func testDatabaseIsClosed() {
        try! database.open()
        XCTAssertNotNil(try? database.close())
        XCTAssertFalse(database.isOpen)
    }
    
    func testStandardUpdateIsExecuted() {
        try! database.open()
        XCTAssertNotNil(try? database.executeUpdate("CREATE TABLE TestTable (integer INTEGER, text TEXT, date INTEGER)"))
    }
    
    func testBindingsUpdateIsExecuted() {
        try! database.open()
        try! database.executeUpdate("CREATE TABLE TestTable (integer INTEGER, text TEXT, date INTEGER)")
        XCTAssertNotNil(try? database.executeUpdate("INSERT INTO TestTable VALUES (?, ?, ?)", bindings: [1, "text", 2]))
    }
    
    func testNamedBindingsUpdateIsExecuted() {
        try! database.open()
        try! database.executeUpdate("CREATE TABLE TestTable (integer INTEGER, text TEXT, date INTEGER)")
        XCTAssertNotNil(try? database.executeUpdate("INSERT INTO TestTable VALUES (:int, :text, :date)", namedBindings: ["int": 1, "text": "text", "date": 2]))
    }
    
    func testContainsTableIsCorrect() {
        try! database.open()
        XCTAssertFalse(try! database.containsTable("TestTable"))
        XCTAssertNotNil(try? database.executeUpdate("CREATE TABLE TestTable (integer INTEGER, text TEXT, date INTEGER)"))
        XCTAssertTrue(try! database.containsTable("TestTable"))
    }
    
    func testBeginsTransaction() {
        try! database.open()
        XCTAssertNotNil(try? database.beginTransaction())
    }

    func testEndsTransaction() {
        try! database.open()
        try? database.beginTransaction()
        XCTAssertNotNil(try? database.endTransaction())
    }
    
    func testRollsBackTransaction() {
        try! database.open()
        XCTAssertNotNil(try? database.beginTransaction())
        XCTAssertNotNil(try? database.rollback())
    }
}
