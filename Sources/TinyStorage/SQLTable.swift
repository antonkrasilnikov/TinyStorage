import Foundation

private class TableOperation: BlockOperation {
    let key: String
    
    init(key:String, block: @escaping () -> Void) {
        self.key = key
        super.init()
        self.addExecutionBlock(block)
    }
}

private class DatabaseOperationQueue: OperationQueue {
    
    static let workQueue: DatabaseOperationQueue = {
        $0.qualityOfService = .userInitiated
        return $0
    }(DatabaseOperationQueue())
    
    static func addOperation(_ op: TableOperation) {
        workQueue.addOperation(op)
    }
    
    func addOperation(_ op: TableOperation) {
        
        let operations = self.operations
        
        for operation in operations {
            if let storageOperation = operation as? TableOperation {
                if storageOperation.key == op.key {
                    op.addDependency(storageOperation)
                }
            }
        }
        super.addOperation(op)
    }
}

public protocol SQLTableEntity: Codable, Equatable {
    associatedtype Entity = Self where Entity:SQLTableEntity
    
    static var template: Entity {get}
    var id: String {get}
    var dictionary: [String: Any]? {get}
}

public extension SQLTableEntity {
    var dictionary: [String: Any]? {
      guard let data = try? JSONEncoder().encode(self) else { return nil }
      return (try? JSONSerialization.jsonObject(with: data, options: .allowFragments)).flatMap { $0 as? [String: Any] }
    }

    static func ==(lhs: Self, rhs: Self) -> Bool {
        return lhs.id == rhs.id
    }
}

enum TableColonType {
    case string
    case int
    case int8
    case int16
    case int32
    case int64
    case uint
    case uint8
    case uint16
    case uint32
    case uint64
    case float
    case double
    case bool
    case object
    
    var isNumber: Bool {
        return self != .string && self != .object
    }
}

public enum SQLTableError: Error {
    case noDatabase
    case wrongTemplate
}

open class SQLTable<T: SQLTableEntity> {
    let name: String
    weak var database: SQLDatabase?
    let params: [String]
    let colons: [String : TableColonType]
    
    public init(name: String, database: SQLDatabase) throws {
        self.name = name
        self.database = database
        
        let temlate = T.template
        
        if let encoded = try? DictionaryEncoder().encode(temlate) {

            var colons: [String : TableColonType] = [:]
            self.params = Array(encoded.keys)
            params.forEach{
                if let value = encoded[$0] {
                    switch value {
                    case _ as String:
                        colons[$0] = .string
                    case _ as Int:
                        colons[$0] = .int
                    case _ as Int8:
                        colons[$0] = .int8
                    case _ as Int16:
                        colons[$0] = .int16
                    case _ as Int32:
                        colons[$0] = .int32
                    case _ as Int64:
                        colons[$0] = .int64
                    case _ as UInt:
                        colons[$0] = .uint
                    case _ as UInt8:
                        colons[$0] = .uint8
                    case _ as UInt16:
                        colons[$0] = .uint16
                    case _ as UInt32:
                        colons[$0] = .uint32
                    case _ as UInt64:
                        colons[$0] = .uint64
                    case _ as Float:
                        colons[$0] = .float
                    case _ as Double:
                        colons[$0] = .double
                    case _ as Bool:
                        colons[$0] = .bool
                    default:
                        colons[$0] = .object
                    }
                }
            }
            
            if params.count != colons.count {
                #if DEBUG
                print("SQLTable create error: template is incorrect", type(of: temlate))
                #endif
                throw SQLTableError.wrongTemplate
            }
            
            self.colons = colons
        }else{
            #if DEBUG
            print("SQLTable create error: template is incorrect", type(of: temlate))
            #endif
            throw SQLTableError.wrongTemplate
        }
        
        try create()
    }
    
    private func formatted(key: String) -> String {
        return "\"" + key.replacingOccurrences(of: "\"", with: "") + "\""
    }
    
    public func addRow(element: T, completion: @escaping ((Bool) -> Void)) {
                
        if let database = database {
            let id = element.id
            
            let operation = TableOperation(key: name) { [weak self] in
                autoreleasepool {
                    guard let self = self else {return}
                    
                    let valuesDct = self.encode(element)
                            
                    var query = "insert or replace into \(self.name) (id"
                    var valueQuery = ") values ('\(id)'"
                    
                    var index = 0
                    
                    for key in self.params {
                        
                        if key != "id", var value = valuesDct[key] {
                            
                            query.append(", ")
                            valueQuery.append(", ")
                            
                            query.append(self.formatted(key: key))
                            value = value.replacingOccurrences(of: "'", with: "''")
                            valueQuery.append("'\(value)'")
                            index += 1
                        }
                        
                    }
                    
                    valueQuery.append(")")
                    query.append(valueQuery)
                    
                    database.executeSetQuery(query: query, completionQueue: nil, completion: completion)
                }
            }
            
            DatabaseOperationQueue.addOperation(operation)
            
        }else{
            completion(false)
        }
    }
    
    public func addRows(elements: [T], completion: @escaping ((Bool) -> Void)) {
        
        if let database = database {
            
            let operation = TableOperation(key: name)  {
                autoreleasepool { [weak self] in
                    
                    guard let self = self else {return}
                    
                    var query = "insert or replace into \(self.name) (id, "
                    
                    var index = 0
                    for key in self.params {
                        guard key != "id"  else {
                            continue
                        }
                        if index > 0 {
                            query.append(", ")
                        }
                        query.append(self.formatted(key: key))
                        index += 1
                    }
                    
                    query.append(") values ")
                    
                    var valueQuery = ""
                    
                    var elementIndex = 0
                    
                    for element in elements {
                        let id = element.id
                        let valuesDct = self.encode(element)
                        
                        valueQuery.append("('\(id)', ")
                        
                        var index = 0
                        
                        for key in self.params {
                            
                            guard key != "id" else {
                                continue
                            }
                            
                            var value = valuesDct[key] ?? ""
                            
                            if index > 0 {
                                valueQuery.append(", ")
                            }
                            
                            value = value.replacingOccurrences(of: "'", with: "''")
                            valueQuery.append("'\(value)'")
                            index += 1
                            
                        }
                        
                        if elementIndex < elements.count - 1 {
                            valueQuery.append("), ")
                        }else {
                            valueQuery.append(")")
                        }
                        
                        elementIndex += 1
                    }
                    
                    query.append(valueQuery)
                    
                    database.executeSetQuery(query: query, completionQueue: nil, completion: completion)
                }
            }
            
            DatabaseOperationQueue.addOperation(operation)
            
        }else{
            completion(false)
        }

    }
    
    public func loadRows(condition:String? = nil, sortedBy key: String? = nil, reverse: Bool = false, from index: Int? = nil, count: Int? = nil, completion: @escaping (([T]) -> Void)) {
        
        if let database = database {
            
            let operation = TableOperation(key: name)  {
                autoreleasepool { [weak self] in
                    guard let self = self else {return}
                    
                    var query = "select * from \(self.name)"
                    
                    if let condition = condition {
                        query.append(" \(condition)")
                    }
                    
                    if let sortedKey = key {
                        query.append(" order by \(self.formatted(key: sortedKey))")
                        
                        if self.colons[sortedKey]?.isNumber == true {
                            query.append(" + 0")
                        }
                        
                        if reverse {
                            query.append(" DESC")
                        }
                    }
                    
                    if let count = count  {
                        query.append("  limit \(index ?? 0), \(count)")
                    }
                    
                    database.runLoadQuery(query: query, completionQueue: DatabaseOperationQueue.workQueue) { (success, objDcts) in
                        var objects: [T] = []
                        
                        if let objDcts = objDcts {
                            for objDct in objDcts {
                                if objDct.count > 0 {
                                    if let obj = self.decode(objDct) {
                                        objects.append(obj)
                                    }
                                }
                            }
                        }
                        
                        OperationQueue.main.addOperation {
                            completion(objects)
                        }
                    }
                }
            }
            
            DatabaseOperationQueue.addOperation(operation)
            
        }else{
            completion([])
        }
        
        
    }
    
    public func loadRows(with keyValue:[String:Any], sortedBy key: String? = nil, reverse: Bool = false, from index: Int? = nil, count: Int? = nil, completion: @escaping (([T]) -> Void)) {
        
        var condition: String? = nil
        
        if keyValue.count > 0 {
            condition = "where"
            var index = 0
            for key in keyValue.keys {
                
                if let value = keyValue[key], let stringValue = stringValue(value) {
                    if index > 0 {
                        condition?.append(" and")
                    }
                    
                    condition?.append(" \(formatted(key: key))='\(stringValue)'")
                    
                    index += 1
                }else{
                    #if DEBUG
                    fatalError("SQLTable error :: wrong value for key \(key)")
                    #endif
                }
            }
        }
        
        loadRows(condition: condition, sortedBy: key, reverse: reverse, from: index, count: count, completion: completion)
        
    }
    
    public func loadRow(rowId:String, completion: @escaping ((T?) -> Void)){
        loadRows(condition: "where id='\(rowId)'") { (rows) in
            completion(rows.first)
        }
    }

    public func delete(element: T, completion: @escaping ((Bool) -> Void)) {
        if let database = database {
            let query = "delete from \(name) where id='\(element.id)'"
            let operation = TableOperation(key: name) {
                autoreleasepool {
                    database.executeSetQuery(query: query, completionQueue: nil, completion: completion)
                }
            }
            DatabaseOperationQueue.addOperation(operation)
        }else{
            completion(false)
        }
    }
    
    public func delete(elements: [T], completion: @escaping ((Bool) -> Void)) {
        
        var query = "delete from \(name) where id in ("
        
        var index = 0
        
        for element in elements {
            if index > 0 {
                query.append(", ")
            }
            query.append("'\(element.id)'")
            
            index += 1
        }
        
        query.append(")")
        
        if index > 0, let database = database {
            let operation = TableOperation(key: name) {
                autoreleasepool {
                    database.executeSetQuery(query: query, completionQueue: nil, completion: completion)
                }
            }
            DatabaseOperationQueue.addOperation(operation)
        }else{
            completion(false)
        }
    }
    
    public func deleteAll(completion: @escaping ((Bool) -> Void)) {
        if let database = database {
            let query = "delete from \(name)"
            let operation = TableOperation(key: name) {
                autoreleasepool {
                    database.executeSetQuery(query: query, completionQueue: nil, completion: completion)
                }
            }
            DatabaseOperationQueue.addOperation(operation)
        }else{
            completion(false)
        }
    }
    
    public func delete(with keyValue:[String:String], completion: @escaping ((Bool) -> Void)) {
                
        if keyValue.count > 0 {
            var condition = "where"
            var index = 0
            for key in keyValue.keys {
                
                if let value = keyValue[key] {
                    if index > 0 {
                        condition.append(" and")
                    }
                    
                    condition.append(" \(formatted(key: key))='\(value)'")
                    
                    index += 1
                }
            }
            
            delete(condition: condition, completion: completion)
        }else{
            completion(false)
        }
    }
    
    public func delete(condition:String, completion: @escaping ((Bool) -> Void)) {
        if let database = database {
            let query = "delete from \(name) \(condition)"
            let operation = TableOperation(key: name) {
                autoreleasepool {
                    database.executeSetQuery(query: query, completionQueue: nil, completion: completion)
                }
            }
            DatabaseOperationQueue.addOperation(operation)
        }else{
            completion(false)
        }
    }
    
    // internal
    
    private func checkDatabaseModelConsistence() {
        
        if let database = database {
            
            var params = self.params
            
            if !params.contains("id") {
                params.append("id")
            }
            
            database.checkTableConsistence(table: name, params: params.compactMap {formatted(key: $0)}, completionQueue: nil) { (_) in
                
            }
        }
    }
    
    private func create() throws {
        if let database = database {
            
            let operation = TableOperation(key: name) {
                autoreleasepool {
                    database.createTable(createQuery: self.createQueryTable(), completionQueue: nil) { (success) in
                        self.checkDatabaseModelConsistence()
                    }
                }
            }
            
            DatabaseOperationQueue.addOperation(operation)
            
        }else{
            #if DEBUG
            print("SQLTable create error: no database")
            #endif
            throw SQLTableError.noDatabase
        }
    }
    
    private func createQueryTable() -> String {
        var str = "CREATE TABLE IF NOT EXISTS \(name) (id text primary key, "
        let keys = params.filter({ $0 == "id" })
        var index = 0
        for key in keys {
            str.append("\(formatted(key: key))" + " text")
            if index < keys.count - 1 {
                str.append(", ")
            }
            index += 1
        }
        str.append(")")
        
        return str
    }
    
    private func stringValue(_ value: Any) -> String? {
        var stringValue: String?
        
        switch value {
        case let value as String:
            stringValue = value
        case let value as Int:
            stringValue = String(value)
        case let value as Int8:
            stringValue = String(value)
        case let value as Int16:
            stringValue = String(value)
        case let value as Int32:
            stringValue = String(value)
        case let value as Int64:
            stringValue = String(value)
        case let value as UInt:
            stringValue = String(value)
        case let value as UInt8:
            stringValue = String(value)
        case let value as UInt16:
            stringValue = String(value)
        case let value as UInt32:
            stringValue = String(value)
        case let value as UInt64:
            stringValue = String(value)
        case let value as Float:
            stringValue = String(value)
        case let value as Double:
            stringValue = String(value)
        case let value as Bool:
            stringValue = value ? "1" : "0"
        default:
            
            if JSONSerialization.isValidJSONObject(value) {
                if let encoded = try? JSONSerialization.data(withJSONObject: value, options: .fragmentsAllowed) {
                    stringValue = String(data: encoded, encoding: .utf8)
                }
            }
        }
        
        return stringValue
    }
    
    private func encode(_ element: T) -> [String:String] {
        
        var valuesDct: [String:String] = [:]
        
        if let objDct = element.dictionary {
            for key in objDct.keys {
                guard key != "id" else {continue}
                
                if let value = objDct[key], let stringValue = stringValue(value) {
                    valuesDct[key] = stringValue
                }else{
                    #if DEBUG
                    print("SQLTable encode error :: unsupported value on key \(key)")
                    #endif
                }
            }
        }
        
        return valuesDct
    }
    
    private func decode(_ valuesDct: [String:String]) -> T? {
        var objDct: [String: Any] = [:]
        
        for key in valuesDct.keys {

            guard let stringValue = valuesDct[key], stringValue.count != 0 else {
                continue
            }
            
            if let type = colons[key] {
                switch type {
                case .string:
                    objDct[key] = stringValue
                case .int:
                    if let value = Int(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .int8:
                    if let value = Int8(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .int16:
                    if let value = Int16(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .int32:
                    if let value = Int32(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .int64:
                    if let value = Int64(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .uint:
                    if let value = UInt(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .uint8:
                    if let value = UInt8(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .uint16:
                    if let value = UInt16(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .uint32:
                    if let value = UInt32(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .uint64:
                    if let value = UInt64(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .float:
                    if let value = Float(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .double:
                    if let value = Double(stringValue) {
                        objDct[key] = NSNumber(value: value)
                    }
                case .bool:
                    if let value = Int(stringValue) {
                        objDct[key] = NSNumber(value: value == 1)
                    }
                case .object:

                    guard let utfData = stringValue.data(using: .utf8) else {
                        objDct[key] = stringValue
                        continue
                    }
                    
                    if let jvalue = try? JSONSerialization.jsonObject(with: utfData, options: .allowFragments) {
                        objDct[key] = jvalue
                    }
                }
            }else if key == "id" {
                objDct[key] = stringValue
            }
        }
        
        return try? JSONDecoder().decode(T.self, from: objDct)
    }
    
}
