//
//  Statement.swift
//  TinySQLite
//
//  Created by Øyvind Grimnes on 25/12/15.
//

import sqlite3

public enum SQLiteDatatype: String {
    case Text       = "TEXT"
    case Integer    = "INTEGER"
    case Real       = "REAL"
    case Blob       = "BLOB"
    case Numeric    = "NUMERIC"
    case Null       = "NULL"
}

public class Statement {
    private var handle: COpaquePointer
    var query: String
    
    var isBusy: Bool {
        return NSNumber(int: sqlite3_stmt_busy(handle)).boolValue
    }
    
    //    TODO: Fix Mappings
    lazy var indexToNameMapping: [Int32: String] = {
        var mapping: [Int32: String] = [:]
        
        for index in 0..<sqlite3_column_count(self.handle) {
            let name =  NSString(UTF8String: sqlite3_column_name(self.handle, index)) as! String
            mapping[index] = name
        }
        
        return mapping
    }()
    
    lazy var nameToIndexMapping: [String: Int32] = {
        var mapping: [String: Int32] = [:]
        
        for index in 0..<sqlite3_column_count(self.handle) {
            let name =  NSString(UTF8String: sqlite3_column_name(self.handle, index)) as! String
            mapping[name] = index
        }
        
        return mapping
    }()
    
    
    public init(_ query: String, handle: COpaquePointer = nil) {
        self.query = query
        self.handle = handle
    }
    
    public func step() throws -> Bool {
        let result = sqlite3_step(handle)
        
        try SQLiteResultHandler.verifyResultCode(result, forHandle: handle)
        
        if result == SQLITE_DONE {
            try finalize()
            return false
        }
        
        return true
    }
    
    public func finalize() throws {
        try SQLiteResultHandler.verifyResultCode(sqlite3_finalize(handle), forHandle: handle)
    }
    
    internal func prepareForDatabase(databaseHandle: COpaquePointer) throws {
        try SQLiteResultHandler.verifyResultCode(sqlite3_prepare_v2(databaseHandle, query, -1, &handle, nil), forHandle: handle)
    }
    
    //    TODO: Merge bind functions
    internal func bind(namedBindings: NamedBindings) throws {
        var parameterNameToIndexMapping: [String: Int32] = [:]
        
        for (name, _) in namedBindings {
            let index = sqlite3_bind_parameter_index(handle, ":\(name)")
            parameterNameToIndexMapping[name] = index
        }
        
        let bindings: Bindings = namedBindings.keys.sort {
            parameterNameToIndexMapping[$0]! > parameterNameToIndexMapping[$1]!
            }.map {namedBindings[$0]!}
        
        try bind(bindings)
    }
    
    internal func bind(bindings: Bindings) throws {
        let totalBindCount = sqlite3_bind_parameter_count(handle)
        
        var bindCount: Int32 = 0
        for (index, value) in bindings.enumerate() {
            try bindValue(value, forIndex: Int32(index+1))
            ++bindCount
        }
        
        if bindCount != totalBindCount {
            throw DatabaseError.Binding(message: "Wrong number of bindings (was '\(bindCount)', should have been '\(totalBindCount)')")
        }
    }
    
    private func bindValue(value: Binding?, forIndex index: Int32) throws {
        if value == nil {
            try SQLiteResultHandler.verifyResultCode(sqlite3_bind_null(handle, index), forHandle: handle)
            return
        }
        
        let result: Int32
        
        switch value {
        case let dateValue as NSDate:
            result = sqlite3_bind_double(handle, index, dateValue.timeIntervalSince1970)
            
        case let integerValue as Int:
            result = sqlite3_bind_int64(handle, index, Int64(integerValue))
            
        case let boolValue as Bool:
            result = sqlite3_bind_int64(handle, index, boolValue ? 1 : 0)
            
        case let floatValue as Float:
            result = sqlite3_bind_double(handle, index, Double(floatValue))
            
        case let doubleValue as Double:
            result = sqlite3_bind_double(handle, index, doubleValue)
            
        case let numberValue as NSNumber:
            result = try bindNumber(numberValue, forIndex: index)
            
        case let stringValue as String:
            result = sqlite3_bind_text(handle, index, stringValue, -1, SQLITE_TRANSIENT)
        case let dataValue as NSData:
            guard dataValue.length > 0 else {
                throw DatabaseError.Binding(message: "Failed to bind NSData value for index \(index). NSData with length = 0 is interperated as NULL in SQLite")
            }
            result = sqlite3_bind_blob(handle, index, dataValue.bytes, -1, SQLITE_TRANSIENT)
        default:
            result = sqlite3_bind_text(handle, index, value as! String, -1, SQLITE_TRANSIENT)
        }
        
        try SQLiteResultHandler.verifyResultCode(result, forHandle: handle)
    }
    
    
    private func bindNumber(numberValue: NSNumber, forIndex index: Int32) throws -> Int32 {
        
        let typeString = String.fromCString(numberValue.objCType)
        if typeString == nil || typeString!.isEmpty {
            throw DatabaseError.Binding(message: "The value wrapped in NSNumber was not recognized. Type string was nil or empty")
        }
        
        let result: Int32
        
        switch typeString! {
        case "c":
            result = sqlite3_bind_int64(handle, index, Int64(numberValue.charValue))
        case "i":
            result = sqlite3_bind_int64(handle, index, Int64(numberValue.intValue))
        case "s":
            result = sqlite3_bind_int64(handle, index, Int64(numberValue.shortValue))
        case "l":
            result = sqlite3_bind_int64(handle, index, Int64(numberValue.longValue))
        case "q":
            result = sqlite3_bind_int64(handle, index, numberValue.longLongValue)
        case "C":
            result = sqlite3_bind_int64(handle, index, Int64(numberValue.charValue))
        case "I":
            result = sqlite3_bind_int64(handle, index, Int64(numberValue.unsignedIntValue))
        case "S":
            result = sqlite3_bind_int64(handle, index, Int64(numberValue.unsignedShortValue))
        case "L":
            result = sqlite3_bind_int64(handle, index, Int64(numberValue.unsignedLongValue))
        case "Q":
            result = sqlite3_bind_int64(handle, index, Int64(numberValue.unsignedLongLongValue))
        case "B":
            result = sqlite3_bind_int64(handle, index, Int64(numberValue.boolValue ? 1 : 0))
        case "f", "d":
            result = sqlite3_bind_double(handle, index, numberValue.doubleValue)
        default:
            result = sqlite3_bind_text(handle, index, numberValue.description, -1, SQLITE_TRANSIENT)
        }
        
        return result
    }
}

//MARK: - Values for indexed columns
extension Statement {
    
    public func typeForColumn(index: Int32) -> SQLiteDatatype {
        switch sqlite3_column_type(handle, index) {
        case SQLITE_INTEGER:
            return .Integer
        case SQLITE_FLOAT:
            return .Real
        case SQLITE_TEXT, SQLITE3_TEXT:
            return .Text
        case SQLITE_BLOB:
            return .Blob
        case SQLITE_NULL:
            return .Null
        default:
            fatalError("Column datatype not configured")
        }
    }
    
    public func typeForColumn(name: String) -> SQLiteDatatype {
        return typeForColumn(nameToIndexMapping[name]!)
    }
    
    public func valueForColumn(index: Int32) -> Binding? {
        let columnType = sqlite3_column_type(handle, index)
        
        switch columnType {
        case SQLITE_INTEGER:
            return integerForColumn(index)
        case SQLITE_FLOAT:
            return doubleForColumn(index)
        case SQLITE_TEXT:
            return stringForColumn(index)
        case SQLITE_BLOB:
            return dataForColumn(index)
        case SQLITE_NULL:
            fallthrough
        default:
            return nil
        }
    }
    
    public func integerForColumn(index: Int32) -> Int? {
        if typeForColumn(index) == .Null {
            return nil
        }
        return Int(sqlite3_column_int64(handle, index))
    }
    
    public func doubleForColumn(index: Int32) -> Double? {
        if typeForColumn(index) == .Null {
            return nil
        }
        return sqlite3_column_double(handle, index)
    }
    
    public func floatForColumn(index: Int32) -> Float? {
        if typeForColumn(index) == .Null {
            return nil
        }
        return doubleForColumn(index) != nil ? Float(doubleForColumn(index)!) : nil
    }
    
    public func boolForColumn(index: Int32) -> Bool? {
        if typeForColumn(index) == .Null {
            return nil
        }
        return integerForColumn(index) != nil ? Bool(integerForColumn(index)!) : nil
    }
    
    public func dataForColumn(index: Int32) -> NSData? {
        if typeForColumn(index) == .Null {
            return nil
        }
        return NSData(bytes: sqlite3_column_blob(handle, index), length: Int(sqlite3_column_bytes(handle, index)))
    }
    
    public func dateForColumn(index: Int32) -> NSDate? {
        if typeForColumn(index) == .Null {
            return nil
        }
        return doubleForColumn(index) != nil ? NSDate(timeIntervalSince1970: doubleForColumn(index)!) : nil
    }
    
    public func stringForColumn(index: Int32) -> String? {
        return nsstringForColumn(index) as? String
    }
    
    public func nsstringForColumn(index: Int32) -> NSString? {
        return NSString(bytes: sqlite3_column_text(handle, index), length: Int(sqlite3_column_bytes(handle, index)), encoding: NSUTF8StringEncoding)
    }
    
    public func numberForColumn(index: Int32) -> NSNumber? {
        switch sqlite3_column_type(handle, index) {
        case SQLITE_INTEGER:
            if let integerValue = integerForColumn(index) {
                return integerValue as NSNumber
            }
            return nil
        case SQLITE_TEXT:
            if let stringValue = stringForColumn(index) {
                return Int(stringValue)
            }
            return nil
        case SQLITE_FLOAT:
            if let doubleValue = doubleForColumn(index) {
                return doubleValue as NSNumber
            }
            return nil
        default:
            return nil
        }
    }
    
    public var dictionary: NamedBindings {
        var dictionary: NamedBindings = [:]
        
        for i in 0..<sqlite3_column_count(handle) {
            dictionary[indexToNameMapping[i]!] = valueForColumn(i)
        }
        
        return dictionary
    }
}

//MARK: - Values for named columns
extension Statement {
    public func valueForColumn(name: String) -> Binding? {
        return valueForColumn(nameToIndexMapping[name]!)
    }
    
    public func integerForColumn(name: String) -> Int? {
        return integerForColumn(nameToIndexMapping[name]!)
    }
    
    public func doubleForColumn(name: String) -> Double? {
        return doubleForColumn(nameToIndexMapping[name]!)
    }
    
    public func floatForColumn(name: String) -> Float? {
        return floatForColumn(nameToIndexMapping[name]!)
    }
    
    public func boolForColumn(name: String) -> Bool? {
        return boolForColumn(nameToIndexMapping[name]!)
    }
    
    public func dataForColumn(name: String) -> NSData? {
        return dataForColumn(nameToIndexMapping[name]!)
    }
    
    public func dateForColumn(name: String) -> NSDate? {
        return dateForColumn(nameToIndexMapping[name]!)
    }
    
    public func stringForColumn(name: String) -> String? {
        return stringForColumn(nameToIndexMapping[name]!)
    }
    
    public func nsstringForColumn(name: String) -> NSString? {
        return nsstringForColumn(nameToIndexMapping[name]!)
    }
    
    public func numberForColumn(name: String) -> NSNumber? {
        return numberForColumn(nameToIndexMapping[name]!)
    }
}

//MARK: - Generator and sequence type
extension Statement: GeneratorType, SequenceType {
    
    public func next() -> Statement? {
        do {
            let result = try step()
            return result ? self : nil
        } catch {
            return nil
        }
    }
    
    public func generate() -> Statement {
        return self
    }
    
}