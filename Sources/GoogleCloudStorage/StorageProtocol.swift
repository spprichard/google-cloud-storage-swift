import Foundation

public protocol StorageProtocol: Sendable {

  func insert(
    data: Data,
    contentType: String,
    object: Object,
    in bucket: Bucket
  ) async throws

  func delete(object: Object, in bucket: Bucket) async throws

  func download(object: Object, in bucket: Bucket) async throws -> Data

  /// List objects in a bucket. Pass `prefix` to limit results to objects whose names start with it
  /// (e.g. `"circana/Geo/exports/"`). Pagination is handled internally — all matching objects are
  /// returned in a single call.
  func list(in bucket: Bucket, prefix: String?) async throws -> [Object]

  func generateSignedURL(
    for action: SignedAction,
    expiration: TimeInterval,
    object: Object,
    in bucket: Bucket
  ) async throws -> String
}

public enum SignedAction {
  case reading
  case writing
}

extension StorageProtocol {

  public func generateSignedURL(
    for action: SignedAction,
    expiration: TimeInterval = Storage.signedURLMaximumExpirationDuration,
    object: Object,
    in bucket: Bucket
  ) async throws -> String {
    try await generateSignedURL(for: action, expiration: expiration, object: object, in: bucket)
  }

  /// Convenience overload that lists every object in the bucket (no prefix filter).
  public func list(in bucket: Bucket) async throws -> [Object] {
    try await list(in: bucket, prefix: nil)
  }
}
