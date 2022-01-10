// trait RayRustSerializer<T> {
//     serialize(data: T, metadata: Option<>) ->
// }
//
// enum SerializeMetadata {
//     Serde
// }
//
//
enum RaySerializer {
    SerdeMsgpack,
    SerdeFlexbuffer,
    SerdeCustom(String),
    Rkyv,
    Fury,
    Custom(String),
}
//
// trait RaySerialize<T> {
//     fn serialize<T>(serializer: Option<RaySerializer>) {
//
//     }
// }
//
// impl<T: serde::Serialize + > RaySerialize<T>
//
// macro_rules! impl_ray_type {
//  (t:ident, ())
// }
//
// #[ray::impl_type(SerdeMsgpack, SerdeFlexbuffer, Rkyv)]
// type Tensor<L, M, N>;
//
// #[ray::impl_type(SerdeMsgpack, SerdeFlexbuffer)]
// type Matrix<M, N>;
