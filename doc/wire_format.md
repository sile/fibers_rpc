Wire Format
===========

Note
----

The endianness of the multi-byte values appear in the document are big endian.


Message Format
--------------

Logically, RPC messages (request/response messages for `Call` RPCs, notification messages for `Cast` RPCs) are
serialized to the following format when transmitted.

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Message Identifier                      |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Procedure Identifier                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Priority    |       Message Payload (Variable Length)
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **Message Identifier (64 bits)**:
  - An identifier is assigned to a message before transmitted.
  - An identifier is unique among all identifiers of the messages used in a TCP connection.
    - As an exception, response message has the same identifier as the corresponding request message.
  - Messages identifiers are hidden from users of the crate.
- **Procedure Identifier (32 bits)**:
  - The identifier of a remote procedure.
  - See also: [`ProcedureId`]
- **Priority (8 bits)**:
  - The priority of a message.
     - The smaller the value, the higher the priority.
  - While higher priority messages are present, transmission of lower priority messages is completely suspended.
- **Message Payload (variable length)**:
  - The payload bytes of a message.

[`ProcedureId`]: https://docs.rs/fibers_rpc/0.2/fibers_rpc/struct.ProcedureId.html


Packet Format
-------------

Physically, RPC messages are split into packets when transmitting.
This packetization prevents huge RPC messages from blocking the transmitting
of other RPC messages sharing the same TCP connection.

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Message Identifier                      |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Procedure Identifier                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Priority    |    Flags      |      Packet Length            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Packet Payload (Variable Length)
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **Message Identifier (64 bits)**, **Procedure Identifier (32 bits)**, **Priority (8 bits)**:
  - See: [Message Format](#Message-Format)
- **Flags (8 bits)**:
  - `END_OF_MESSAGE_FLAG (mask=0b0000_0001)`:
    - If the bit is set, it indicates that it is the last packet needed for reconstructing a message.
  - `ASYNC_FLAG (mask=0b0000_0010)`:
    - If the bit is set, it indicates that encoding and decoding of the packet should be executed in
      a different thread than the [fibers] scheduler threads.
    - [bytecodec] supports incremental encoding/decoding, but it is the responsibility of the user to actually implement encoders/decoders in incremental.
      - Especially, [serde] based encoders/decoders only supports monolithic encoding/decoding.
- **Packet Length (16 bits)**:
  - Number of bytes of the payload of the packet.
- **Packet Payload (variable length)**:
  - It contains a fragment of the payload of a message.


[bytecodec]: https://github.com/sile/bytecodec
[fibers]: https://github.com/dwango/fibers-rs
[serde]: https://crates.io/crates/serde
