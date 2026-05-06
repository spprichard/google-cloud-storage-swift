import Foundation
import Tracing

extension Storage {

  // MARK: - Insert

  public func insert(data: Data, contentType: String, object: Object, in bucket: Bucket)
    async throws
  {
    try await withSpan("storage-insert", ofKind: .client) { span in
      span.attributes["storage/bucket"] = bucket.name
      try await execute(
        method: .POST,
        path: "/upload/storage/v1/b/\(bucket.urlEncoded)/o",
        queryItems: [
          .init(name: "uploadType", value: "media"),
          .init(name: "name", value: object.path),
        ],
        headers: [
          "Content-Type": contentType,
          "Content-Length": String(data.count),
        ],
        body: .bytes(data)
      )
    }
  }

  // MARK: - Download

  public func download(object: Object, in bucket: Bucket) async throws -> Data {
    try await withSpan("storage-download", ofKind: .client) { span in
      span.attributes["storage/bucket"] = bucket.name
      return try await executeData(
        method: .GET,
        path: "/storage/v1/b/\(bucket.urlEncoded)/o/\(object.urlEncoded)",
        queryItems: [.init(name: "alt", value: "media")]
      )
    }
  }

  // MARK: - Delete

  public func delete(object: Object, in bucket: Bucket) async throws {
    try await withSpan("storage-delete", ofKind: .client) { span in
      span.attributes["storage/bucket"] = bucket.name
      try await execute(
        method: .DELETE,
        path: "/storage/v1/b/\(bucket.urlEncoded)/o/\(object.urlEncoded)"
      )
    }
  }
    
  public func list(in bucket: Bucket, prefix: String? = nil) async throws -> [Object] {
    try await withSpan("storage-list", ofKind: .client) { span in
      span.attributes["storage/bucket"] = bucket.name
      if let prefix {
        span.attributes["storage/prefix"] = prefix
      }
      var allObjects: [Object] = []
      var pageToken: String? = nil
      repeat {
        var queryItems: [URLQueryItem] = []
        if let prefix {
          queryItems.append(.init(name: "prefix", value: prefix))
        }
        if let pageToken {
          queryItems.append(.init(name: "pageToken", value: pageToken))
        }
        let response: ListResponse = try await execute(
          method: .GET,
          path: "/storage/v1/b/\(bucket.urlEncoded)/o",
          queryItems: queryItems.isEmpty ? nil : queryItems
        )
        allObjects.append(contentsOf: (response.items ?? []).map { Object(path: $0.name) })
        pageToken = response.nextPageToken
      } while pageToken != nil
      return allObjects
    }
  }
}

private struct ListResponse: Decodable {
  struct Item: Decodable {
    let name: String
  }
  let items: [Item]?
  let nextPageToken: String?
}
