//! Android certificate verification setup for embedded networking.

use crate::{Error, Result};
use std::ffi::c_void;

/// Initialize the Android certificate verifier before any network client is started.
/// The host must bundle the matching rustls-platform-verifier Java helper classes.
///
/// # Safety
/// `raw_env` must be the current thread's valid JNI environment. `raw_context` must be a
/// live Android application context reference belonging to that environment. This function
/// must be called from an active native method frame, without an outstanding JNI exception.
pub unsafe fn initialize_android_verifier(
    raw_env: *mut c_void,
    raw_context: *mut c_void,
) -> Result<()> {
    if raw_env.is_null() || raw_context.is_null() {
        return Err(Error::Invalid("Android network context is missing"));
    }
    // The host provides a current native method frame, as required by the safety contract.
    let mut unowned = unsafe { jni::EnvUnowned::from_raw(raw_env.cast()) };
    let outcome = unowned.with_env(|env| {
        // The reference remains valid for this native frame and the verifier retains globals.
        let context = unsafe { jni::objects::JObject::from_raw(env, raw_context.cast()) };
        let result = rustls_platform_verifier::android::init_with_env(env, context);
        if result.is_err() {
            env.exception_clear();
        }
        result
    });
    match outcome.into_outcome() {
        jni::Outcome::Ok(()) => Ok(()),
        _ => Err(Error::Invalid(
            "Android certificate verifier initialization failed",
        )),
    }
}
