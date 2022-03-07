//
//  PersistentStorage.swift

import Foundation

private class StorageOperation: BlockOperation {
    let key: String
    
    init(key:String, block: @escaping () -> Void) {
        self.key = key
        super.init()
        self.addExecutionBlock(block)
    }
}

private class StorageOperationQueue: OperationQueue {
    func addOperation(_ op: StorageOperation) {
        
        let operations = self.operations
        
        for operation in operations {
            if let storageOperation = operation as? StorageOperation {
                if storageOperation.key == op.key {
                    op.addDependency(storageOperation)
                }
            }
        }
        super.addOperation(op)
    }
}

public class PersistentStorage {

    private let queue = StorageOperationQueue()
    
    public init() {
        
    }
    
    public func getData<T:Decodable>(path: String, completionQueue: OperationQueue = OperationQueue.main, completion: @escaping ((T?) -> Void)) {
        let operation = StorageOperation.init(key: path) {
            autoreleasepool {
                guard FileManager.default.fileExists(atPath: path),
                let data = FileManager.default.contents(atPath: path) else {
                    completionQueue.addOperation {
                        completion(nil)
                    }
                    return;
                }
                
                let decoder = JSONDecoder()
                let obj = try? decoder.decode(T.self, from: data)
                
                completionQueue.addOperation {
                    completion(obj)
                }
            }
        }
        queue.addOperation(operation)
    }
    
    public func save<T:Encodable>(object: T, path: String, completionQueue: OperationQueue = OperationQueue.main, completion: @escaping ((Bool) -> Void)) {
        let operation = StorageOperation.init(key: path) {
            autoreleasepool {
                let encoder = JSONEncoder()
                var success = true
                if let data = try? encoder.encode(object) {
                    do {
                        let url = URL.init(fileURLWithPath: path)
                        try data.write(to: url)
                    } catch  {
                        success = false
                    }
                }else {
                    success = false
                }
                completionQueue.addOperation {
                    completion(success)
                }
            }
        }
        queue.addOperation(operation)
    }
    
    public func getRawData(path: String, completionQueue: OperationQueue = OperationQueue.main, completion: @escaping ((Data?) -> Void)) {
        let operation = StorageOperation.init(key: path) {
            autoreleasepool {
                guard FileManager.default.fileExists(atPath: path) else {
                    completionQueue.addOperation {
                        completion(nil)
                    }
                    return;
                }
                
                let data = FileManager.default.contents(atPath: path)
                
                completionQueue.addOperation {
                    completion(data)
                }
            }
        }
        queue.addOperation(operation)
    }
    
    public func saveRaw(data: Data, path: String, completionQueue: OperationQueue = OperationQueue.main, completion: @escaping ((Bool) -> Void)) {
        let operation = StorageOperation.init(key: path) {
            autoreleasepool {
                var success = true
                do {
                    let url = URL.init(fileURLWithPath: path)
                    try data.write(to: url)
                } catch  {
                    success = false
                }
                completionQueue.addOperation {
                    completion(success)
                }
            }
        }
        queue.addOperation(operation)
    }
    
    public func delete(path: String) {
        let operation = StorageOperation.init(key: path) {
            guard FileManager.default.fileExists(atPath: path) else {
                return;
            }
            try? FileManager.default.removeItem(atPath: path)
        }
        queue.addOperation(operation)
    }
    
}
