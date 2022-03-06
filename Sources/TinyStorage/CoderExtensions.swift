import Foundation

public extension JSONDecoder {
    func decode<T>(_ type: T.Type, from dictionary: [String: Any]) throws -> T where T : Decodable {
        let data = try JSONSerialization.data(withJSONObject: dictionary, options: [])
        return try self.decode(type, from: data)
    }
    
    static func model<T: Decodable>(from dictionary: [String:Any], type: T.Type) -> T? {
        return try? JSONDecoder().decode(type, from: dictionary)
    }
}

public extension JSONEncoder {
    func encodeToJSONObject<T>(_ value: T) throws -> Any where T : Encodable {
        let data = try encode(value)
        return try JSONSerialization.jsonObject(with: data, options: .allowFragments)
    }
}

public extension KeyedDecodingContainerProtocol {
    
    func decodeInt(forKey key: Self.Key) throws -> Int {
        let number : Int
        do {
            number = try decode(Int.self, forKey: key)
        } catch DecodingError.typeMismatch(let type, let context) {
            do {
                let strValue = try decode(String.self, forKey: key)
                if let intValue = Int(strValue) {
                    number = intValue
                }else
                {
                    throw DecodingError.typeMismatch(type, context)
                }
            } catch  {
                throw DecodingError.typeMismatch(type, context)
            }
        } catch {
            throw error
        }
        return number
    }
    
    func decodeInt64(forKey key: Self.Key) throws -> Int64 {
        let number : Int64
        do {
            number = try decode(Int64.self, forKey: key)
        } catch DecodingError.typeMismatch(let type, let context) {
            do {
                let strValue = try decode(String.self, forKey: key)
                if let intValue = Int64(strValue) {
                    number = intValue
                }else
                {
                    throw DecodingError.typeMismatch(type, context)
                }
            } catch  {
                throw DecodingError.typeMismatch(type, context)
            }
        } catch {
            throw error
        }
        return number
    }
    
    func decodeFloat(forKey key: Self.Key) throws -> Float {
        let number : Float
        do {
            number = try decode(Float.self, forKey: key)
        } catch DecodingError.typeMismatch(let type, let context) {
            do {
                let intValue = try decodeInt(forKey: key)
                number = Float(intValue)
            } catch {
                throw DecodingError.typeMismatch(type, context)
            }
            
        } catch {
            throw error
        }
        return number
    }
    
    func decodeDouble(forKey key: Self.Key) throws -> Double {
        let number : Double
        do {
            number = try decode(Double.self, forKey: key)
        } catch DecodingError.typeMismatch(let type, let context) {
            do {
                let intValue = try decodeInt(forKey: key)
                number = Double(intValue)
            } catch {
                throw DecodingError.typeMismatch(type, context)
            }
            
        } catch {
            throw error
        }
        return number
    }
    
    func decodeBool(forKey key: Self.Key) throws -> Bool {
        let number : Bool
        do {
            number = try decode(Bool.self, forKey: key)
        } catch DecodingError.typeMismatch(let type, let context) {
            do {
                let intValue = try decodeInt(forKey: key)
                if intValue == 1 {
                    number = true
                }else if intValue == 0 {
                    number = false
                }else {
                    throw DecodingError.typeMismatch(type, context)
                }
            } catch {
                throw DecodingError.typeMismatch(type, context)
            }
            
        } catch {
            throw error
        }
        return number
    }
    
    func decodeString(forKey key: Self.Key) throws -> String {
        let string : String
        do {
            string = try decode(String.self, forKey: key)
        } catch DecodingError.typeMismatch(let type, let context) {
            do {
                let intValue = try decode(Int.self, forKey: key)
                string = String(intValue);
            } catch  {
                
                do {
                    let floatValue = try decode(Float.self, forKey: key)
                    string = String(floatValue)
                } catch {
                    throw DecodingError.typeMismatch(type, context)
                }
            }
        } catch {
            throw error
        }
        return string
    }
}
