# AegisDB Protobuf API Breaking Change Policy

To ensure backwards compatibility during rolling upgrades and zero-downtime deployments, AegisDB follows a strict policy for `.proto` files:

1. **Never change the tag number of any existing field.**
2. **Never reuse a tag number.** When you delete a field, use the `reserved` keyword to prevent the tag number from being reused by mistake.
3. **Never reuse a field name.** When you delete a field, reserve its name as well to prevent JSON serialization conflicts.
4. **Always add new fields with new tag numbers.** New fields must be marked as optional (or left as proto3 default) so that older clients can still parse messages.
5. **Never change the type of a field.** If you need a different type, create a new field with a new tag number.

## Example of Deprecation
```protobuf
message AppendEntriesArgs {
  // Reserved fields that were removed in previous versions
  reserved 3, 5;
  reserved "old_term", "deprecated_flag";

  uint64 term = 1;
  string leader_id = 2;
  // uint64 old_term = 3; // DELETED
  // ...
}
```
