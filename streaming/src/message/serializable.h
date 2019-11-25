#ifndef RAY_SERIALIZABLE_H
#define RAY_SERIALIZABLE_H

#include <cstdlib>

namespace ray {
namespace streaming {
// StreamingSerializable is supper class of message and bundle.
// We assume there are specific implementation of serilize & deserlize functions.
// Actually, serilization is member function and deserilization is
// implemented as static function for creating object.
class StreamingSerializable {
  virtual void ToBytes(uint8_t *) = 0;
  virtual uint32_t ClassBytesSize() = 0;
};

#define STREAMING_SERIALIZATION_LENGTH inline virtual uint32_t ClassBytesSize()
#define GET_STREAMING_SERIALIZATION_LENGTH(OBJ) OBJ->ClassBytesSize()

#define STREAMING_SERIALIZATION virtual void ToBytes(uint8_t *);
#define STREAMING_SERIALIZATION_IMP(CLASS_NAME, BYTES) \
  void CLASS_NAME::ToBytes(uint8_t *BYTES)

#define STREAMING_DESERIALIZATION(CLASS_PTR) \
  static CLASS_PTR FromBytes(const uint8_t *, bool verifer_check = true);
#define STREAMING_DESERIALIZATION_IMP(CLASS_NAME, CLASS_PTR, BYTES) \
  CLASS_PTR                                                         \
  CLASS_NAME::FromBytes(const uint8_t *BYTES, bool verifer_check)
}  // namespace streaming
}  // namespace ray

#endif  // RAY_SERIALIZABLE_H
