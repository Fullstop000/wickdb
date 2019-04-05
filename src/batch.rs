use crate::util::slice::Slice;
use crate::util::status::Status;

/// `WriteBatch` holds a collection of updates to apply atomically to a DB.
///
/// The updates are applied in the order in which they are added
/// to the `WriteBatch`.
///
/// Multiple threads can invoke all methods on a `WriteBatch` without
/// external synchronization, but if any of the threads may call a
/// non-const method, all threads accessing the same WriteBatch must use
/// external synchronization.
pub struct  WriteBatch {
}

impl WriteBatch {
    fn put(&mut self, key: Slice, value: Slice) {

    }
    fn delete(&mut self, key: Slice) {

    }

    fn get(&self, key: Slice) -> Result<(), Status> {
        Ok(())
    }

    fn apply(&mut self, batch: WriteBatch) {

    }
}
