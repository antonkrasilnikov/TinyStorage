import Foundation

open class Model<CodingKeysT: CaseIterable>: Codable, Equatable, CustomStringConvertible, NSCopying, SQLTableEntity  {
    open var id : String
    
    private enum CodingKeys: String, CodingKey {
        case id
    }
    
    public static func codingKeys() -> [String] {
        return CodingKeysT.allCases.compactMap { key -> String? in
            if let key = key as? CodingKey {
                return key.stringValue
            }
            return nil
        }
    }
    
    public static var template: Model<CodingKeysT> {
        return mock()
    }
    
    open class func mock() -> Self {
        fatalError()
    }
    
    public static func ==(lhs: Model, rhs: Model) -> Bool {
        return lhs.id == rhs.id
    }
    
    open var description: String {
        return String(describing: type(of: self)) + " " + "id:" + id
    }
    
    public init(id: String) {
        self.id = id
    }
    
    public required init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.id = try container.decodeString(forKey: .id)
    }
    
    open func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
    }
    
    open func copy(with zone: NSZone? = nil) -> Any {
        let dataObj = try! JSONEncoder().encode(self)
        return try! JSONDecoder().decode(type(of: self), from: dataObj)
    }
    
    open func sync(with model: Model) {
        
    }
}
