import Foundation
import SQLite3

public protocol SQLDatabase: AnyObject {
    func createTable(createQuery: String, completionQueue: OperationQueue?, completion: @escaping ((Bool) -> Void))
    func executeSetQuery(query: String, completionQueue: OperationQueue?, completion: @escaping ((Bool) -> Void))
    func runLoadQuery(query: String, completionQueue: OperationQueue?, completion: @escaping ((Bool, [[String:String]]?) -> Void))
    func checkTableConsistence(table name: String, params: [String], completionQueue: OperationQueue?, completion: @escaping ((Bool) -> Void))
}

public class Database: SQLDatabase {
    
    private let dbPath: String
    private let queue = OperationQueue()
    private var sqlDB: OpaquePointer?
    
    public init(databasePath: String) {
        dbPath = databasePath
        queue.maxConcurrentOperationCount = 1
    }
    
    deinit {
        closeDatabase()
    }
    
    public func configure(completion: @escaping ((Bool) -> Void)) {
        openDatabase(completion: completion)
    }
    
    private func printLastError() {
        #if DEBUG
        let errorMessage = String(cString: sqlite3_errmsg(sqlDB))
        print("Database error ::",errorMessage)
        #endif
    }
    
    private func openDatabase(completionQueue: OperationQueue? = nil, completion: @escaping ((Bool) -> Void)) {
        queue.addOperation {
            autoreleasepool { [weak self] in
                guard let self = self else {return}
                
                var errorBehaviour = false
                
                if self.sqlDB == nil {
                    if sqlite3_open(self.dbPath, &(self.sqlDB)) != SQLITE_OK {
                        self.printLastError()
                        errorBehaviour = true
                    }
                }else{
                    errorBehaviour = true
                }
                
                let completionQueue = completionQueue ?? OperationQueue.main
                
                completionQueue.addOperation {
                    completion(!errorBehaviour)
                }
            }
        }
    }
    
    private func closeDatabase() {
        if let db = sqlDB {
            queue.addOperation {
                sqlite3_close(db)
            }
            queue.waitUntilAllOperationsAreFinished()
        }
    }
    
    public func runLoadQuery(query: String, completionQueue: OperationQueue? = nil, completion: @escaping ((Bool, [[String:String]]?) -> Void)) {
        
        queue.addOperation {
            
            autoreleasepool { [weak self] in
                guard let self = self else {return}
                
                var errorBehaviour = false
                var objects: [[String:String]] = []
                
                if let database = self.sqlDB {
                    
                    var compiledStatement: OpaquePointer? = nil
                    
                    if sqlite3_prepare_v2(database, query, -1, &compiledStatement, nil) == SQLITE_OK {
                        
                        while sqlite3_step(compiledStatement) == SQLITE_ROW {
                            var objDct: [String:String] = [:]
                            let totalColumns = sqlite3_column_count(compiledStatement)
                            
                            for i in 0..<totalColumns {
                                
                                if let dbDataAsChars = sqlite3_column_text(compiledStatement, i),
                                   let dbColumnAsChars = sqlite3_column_name(compiledStatement, i) {
                                    let column = String(cString: dbColumnAsChars)
                                    let data = String(cString: dbDataAsChars)
                                    
                                    objDct[column] = data
                                }
                            }
                            if objDct.count > 0 {
                                objects.append(objDct)
                            }
                        }

                    }else {
                        self.printLastError()
                        errorBehaviour = true
                    }
                    
                    sqlite3_finalize(compiledStatement)
                    
                }else{
                    errorBehaviour = true
                }
                
                let completionQueue = completionQueue ?? OperationQueue.main
                
                completionQueue.addOperation {
                    if errorBehaviour {
                        completion(false,nil)
                    }else {
                        completion(true,objects)
                    }
                }
            }

        }
    }
    
    public func executeSetQuery(query: String, completionQueue: OperationQueue? = nil, completion: @escaping ((Bool) -> Void)) {
        
        queue.addOperation {
            
            autoreleasepool { [weak self] in
                guard let self = self else {return}
                
                var errorBehaviour = false
                
                if let database = self.sqlDB {
                    var compiledStatement: OpaquePointer? = nil
                    
                    if sqlite3_prepare_v2(database, query, -1, &compiledStatement, nil) == SQLITE_OK {
                        if sqlite3_step(compiledStatement) != SQLITE_DONE {
                            self.printLastError()
                            errorBehaviour = true
                        }
                    }else {
                        self.printLastError()
                        errorBehaviour = true
                    }
                    
                    sqlite3_finalize(compiledStatement)
                }else {
                    errorBehaviour = true
                }
                
                let completionQueue = completionQueue ?? OperationQueue.main
                
                completionQueue.addOperation {
                    completion(!errorBehaviour)
                }
                
            }
            
        }
    }
    
    public func createTable(createQuery: String, completionQueue: OperationQueue? = nil, completion: @escaping ((Bool) -> Void)) {
        queue.addOperation {
            
            autoreleasepool { [weak self] in
                guard let self = self else {return}
                
                var errorBehaviour = false
                
                if let database = self.sqlDB {
                    if sqlite3_exec(database, createQuery, nil, nil, nil) != SQLITE_OK {
                        self.printLastError()
                        errorBehaviour = true
                    }
                }else{
                    errorBehaviour = true
                }
                
                let completionQueue = completionQueue ?? OperationQueue.main
                
                completionQueue.addOperation {
                    completion(!errorBehaviour)
                }
                
            }
            
        }
    }
    
    public func checkTableConsistence(table name: String, params: [String], completionQueue: OperationQueue? = nil, completion: @escaping ((Bool) -> Void)) {
        
        queue.addOperation {
            
            autoreleasepool { [weak self] in
                guard let self = self else {return}
                
                var errorBehaviour = false
                
                if let database = self.sqlDB {
                    
                    let query = "PRAGMA table_info(\(name))"
                    
                    var keys: [String] = []
                                
                    var compiledStatement: OpaquePointer? = nil
                    
                    if sqlite3_prepare_v2(database, query, -1, &compiledStatement, nil) == SQLITE_OK {
                        
                        while sqlite3_step(compiledStatement) == SQLITE_ROW {
                            var objDct: [String:String] = [:]
                            let totalColumns = sqlite3_column_count(compiledStatement)
                            for i in 0..<totalColumns {
                                
                                if let dbDataAsChars = sqlite3_column_text(compiledStatement, i),
                                   let dbColumnAsChars = sqlite3_column_name(compiledStatement, i) {
                                    let column = String(cString: dbColumnAsChars)
                                    let data = String(cString: dbDataAsChars)
                                    
                                    objDct[column] = data
                                }
                            }
                            if let colomnName = objDct["name"] {
                                keys.append(colomnName)
                            }
                        }
                    }else {
                        self.printLastError()
                        errorBehaviour = true
                    }
                    
                    for param in params {
                        if !keys.contains(param.replacingOccurrences(of: "\"", with: "")) {
                            let query = "ALTER TABLE \(name) ADD COLUMN \(param) text"
                            
                            if sqlite3_prepare_v2(database, query, -1, &compiledStatement, nil) == SQLITE_OK {
                                if sqlite3_step(compiledStatement) != SQLITE_DONE {
                                    self.printLastError()
                                    errorBehaviour = true
                                }
                            }else {
                                self.printLastError()
                                errorBehaviour = true
                            }
                        }
                    }
                    
                    sqlite3_finalize(compiledStatement)
                }else{
                    errorBehaviour = true
                }
                
                let completionQueue = completionQueue ?? OperationQueue.main
                
                completionQueue.addOperation {
                    completion(!errorBehaviour)
                }
            }
        }
    }
}
