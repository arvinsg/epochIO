//! Role assembly. M4 implements the `data` and `pd` roles; M5 adds `meta`;
//! M6 adds `gateway` (the S3 head).

pub mod data;
pub mod gateway;
pub mod gc;
pub mod maintenance;
pub mod meta;
pub mod meta_admin;
pub mod object_browser;
pub mod pd;
pub mod repair;
