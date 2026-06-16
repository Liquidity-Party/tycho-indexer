pub mod cli;
pub mod extractor;
#[allow(clippy::result_large_err)]
pub mod pb;
pub mod services;
pub mod substreams;

#[cfg(test)]
#[allow(clippy::extra_unused_lifetimes)]
mod testing;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;
