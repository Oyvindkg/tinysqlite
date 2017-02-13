//
//  ResultHandlerTests.swift
//  TinySQLite
//
//  Created by Øyvind Grimnes on 13/02/17.
//  Copyright © 2017 Øyvind Grimnes. All rights reserved.
//

import XCTest
import Foundation
import Nimble
import sqlite3


@testable import TinySQLite

class ResultHandlerTests: XCTestCase {
    
    func testResultDoesNotThrowErrorForOK() {
        expect(try ResultHandler.verifyResult(code: SQLITE_OK)).notTo(throwError())
    }
    
    func testResultDoesNotThrowErrorForDONE() {
        expect(try ResultHandler.verifyResult(code: SQLITE_DONE)).notTo(throwError())
    }
    
    func testResultDoesNotThrowErrorForROW() {
        expect(try ResultHandler.verifyResult(code: SQLITE_ROW)).notTo(throwError())
    }
    
    func testResultThrowsErrorForERROR() {
        expect(try ResultHandler.verifyResult(code: SQLITE_ERROR)).to(throwError())
    }
    
    func testResultThrowsErrorForINTERNAL() {
        expect(try ResultHandler.verifyResult(code: SQLITE_INTERNAL)).to(throwError())
    }
    
    func testResultThrowsErrorForPERM() {
        expect(try ResultHandler.verifyResult(code: SQLITE_PERM)).to(throwError())
    }
    
    func testResultThrowsErrorForABORT() {
        expect(try ResultHandler.verifyResult(code: SQLITE_ABORT)).to(throwError())
    }
    
    func testResultThrowsErrorForBUSY() {
        expect(try ResultHandler.verifyResult(code: SQLITE_BUSY)).to(throwError())
    }
    
    func testResultThrowsErrorForLOCKED() {
        expect(try ResultHandler.verifyResult(code: SQLITE_LOCKED)).to(throwError())
    }
    
    func testResultThrowsErrorForNOMEM() {
        expect(try ResultHandler.verifyResult(code: SQLITE_NOMEM)).to(throwError())
    }
    
    func testResultThrowsErrorForREADONLY() {
        expect(try ResultHandler.verifyResult(code: SQLITE_READONLY)).to(throwError())
    }
    
    func testResultThrowsErrorForINTERRUPT() {
        expect(try ResultHandler.verifyResult(code: SQLITE_INTERRUPT)).to(throwError())
    }
    
    func testResultThrowsErrorForIOERR() {
        expect(try ResultHandler.verifyResult(code: SQLITE_IOERR)).to(throwError())
    }
    
    func testResultThrowsErrorForCORRUPT() {
        expect(try ResultHandler.verifyResult(code: SQLITE_CORRUPT)).to(throwError())
    }
    
    func testResultThrowsErrorForNOTFOUND() {
        expect(try ResultHandler.verifyResult(code: SQLITE_NOTFOUND)).to(throwError())
    }
    
    func testResultThrowsErrorForFULL() {
        expect(try ResultHandler.verifyResult(code: SQLITE_FULL)).to(throwError())
    }
    
    func testResultThrowsErrorForCANTOPEN() {
        expect(try ResultHandler.verifyResult(code: SQLITE_CANTOPEN)).to(throwError())
    }
    
    func testResultThrowsErrorForPROTOCOL() {
        expect(try ResultHandler.verifyResult(code: SQLITE_PROTOCOL)).to(throwError())
    }
    
    func testResultThrowsErrorForEMPTY() {
        expect(try ResultHandler.verifyResult(code: SQLITE_EMPTY)).to(throwError())
    }
    
    func testResultThrowsErrorForSCHEMA() {
        expect(try ResultHandler.verifyResult(code: SQLITE_SCHEMA)).to(throwError())
    }
    
    func testResultThrowsErrorForTOOBIG() {
        expect(try ResultHandler.verifyResult(code: SQLITE_TOOBIG)).to(throwError())
    }
    
    func testResultThrowsErrorForCONSTRAINT() {
        expect(try ResultHandler.verifyResult(code: SQLITE_CONSTRAINT)).to(throwError())
    }
    
    func testResultThrowsErrorForMISMATCH() {
        expect(try ResultHandler.verifyResult(code: SQLITE_MISMATCH)).to(throwError())
    }
    
    func testResultThrowsErrorForMISUSE() {
        expect(try ResultHandler.verifyResult(code: SQLITE_MISUSE)).to(throwError())
    }
    
    func testResultThrowsErrorForNOLFS() {
        expect(try ResultHandler.verifyResult(code: SQLITE_NOLFS)).to(throwError())
    }
    
    func testResultThrowsErrorForAUTH() {
        expect(try ResultHandler.verifyResult(code: SQLITE_AUTH)).to(throwError())
    }
    
    func testResultThrowsErrorForFORMAT() {
        expect(try ResultHandler.verifyResult(code: SQLITE_FORMAT)).to(throwError())
    }
    
    func testResultThrowsErrorForRANGE() {
        expect(try ResultHandler.verifyResult(code: SQLITE_RANGE)).to(throwError())
    }
    
    func testResultThrowsErrorForNOTADB() {
        expect(try ResultHandler.verifyResult(code: SQLITE_NOTADB)).to(throwError())
    }
    
    func testResultThrowsErrorForNOTICE() {
        expect(try ResultHandler.verifyResult(code: SQLITE_NOTICE)).to(throwError())
    }
    
    func testResultThrowsErrorForWARNING() {
        expect(try ResultHandler.verifyResult(code: SQLITE_WARNING)).to(throwError())
    }
}
