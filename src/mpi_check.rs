//! Test-only assertions for Hataori-owned MPI call sites.

#[cfg(test)]
#[inline]
pub(crate) fn assert_thread_main() {
    let mut flag = 0;
    // Keep the direct query in this module so the call-boundary scanner has one
    // intentional exception and every wrapped operation gets the same check.
    unsafe { crate::mpi_backend::ffi::MPI_Is_thread_main(&mut flag) };
    assert_ne!(flag, 0, "Hataori MPI call ran off the MPI main thread");
}

#[inline]
pub(crate) fn is_thread_main() -> bool {
    let mut flag = 0;
    unsafe { crate::mpi_backend::ffi::MPI_Is_thread_main(&mut flag) };
    flag != 0
}

macro_rules! mpi_call {
    ($operation:expr) => {{
        #[cfg(test)]
        $crate::mpi_check::assert_thread_main();
        $operation
    }};
}

pub(crate) use mpi_call;
