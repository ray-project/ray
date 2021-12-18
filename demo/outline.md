1. python -> java: cross language, strongly typed
   java -> python: cross language, python dynamic import

2. python -> python: schema evolution

V1:

```protobuf
message Request {
    int age = 1;
}
message Response {
    boolean drink_ok = 1;
}
```

V2:

```protobuf
message Request {
    int age = 1;
    string name = 2;
}
message Response {
    boolean drink_ok = 1;
    string error_message = 2;
}
```

client v1, actor v1
client v2, actor v1 (error_message will now be empty)
client v1, actor v2 (name empty, actor will recognize that)
client v2, actor v2
