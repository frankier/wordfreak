use std::slice::Chunks;


pub fn partition<'a, T>(slice: &'a [T], num_slices: usize) -> Chunks<'a, T> {
    slice.chunks((slice.len() + num_slices - 1) / num_slices)
}
