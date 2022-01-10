use ray_rs_sys::*;

remote! {
pub fn add_two_vecs(a: Vec<u64>, b: Vec<u64>) -> Vec<u64> {
    assert_eq!(a.len(), b.len());
    let mut ret = vec![0u64; a.len()];
    for i in 0..a.len() {
        ret[i] = a[i] + b[i];
    }
    ret
}
}

remote! {
pub fn put_and_get_nested(a: Vec<u64>) -> Vec<u64> {
    ray_api_ffi::LogInfo("HERE 1");
    let id = put::<Vec<u64>, _>(&a);
    ray_api_ffi::LogInfo("HERE 2");
    let a_get = get::<Vec<u64>>(id);

    ray_api_ffi::LogInfo("HERE 3");
    assert_eq!(a, a_get);

    ray_api_ffi::LogInfo("HERE 4");
    return a_get;
}
}

remote! {
pub fn add_two_vecs_nested(a: Vec<u64>, b: Vec<u64>) -> Vec<u64> {
    let objr = add_two_vecs.remote(&a, &b);
    ray_api_ffi::LogInfo("SUBMITTED REMOTE FN");
    get(objr)
}
}

// remote! {
// pub fn add_two_vecs_nested_remote_outer_get(a: Vec<u64>, b: Vec<u64>) -> Vec<u8> {
//     let objr = add_two_vecs.remote(&a, &b);
//     object_id_to_byte_vec(objr)
// }
// }

remote! {
pub fn add_three_vecs(a: Vec<u64>, b: Vec<u64>, c: Vec<u64>) -> Vec<u64> {
    assert_eq!(a.len(), b.len());
    assert_eq!(a.len(), c.len());
    let mut ret = vec![0u64; a.len()];
    for i in 0..a.len() {
        ret[i] = a[i] + b[i] + c[i];
    }
    ret
}
}
