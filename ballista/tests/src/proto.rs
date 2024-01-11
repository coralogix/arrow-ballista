#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TestTable {
    #[prost(uint64, tag = "1")]
    pub parallelism: u64,
    #[prost(uint64, tag = "2")]
    pub global_limit: u64,
    #[prost(string, tag = "3")]
    pub state_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "4")]
    pub value: u32,
    #[prost(bool, tag = "5")]
    pub preempt_stage: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TestTableExec {
    #[prost(message, optional, tag = "1")]
    pub table: ::core::option::Option<TestTable>,
    #[prost(uint64, optional, tag = "2")]
    pub limit: ::core::option::Option<u64>,
    #[prost(uint64, repeated, tag = "3")]
    pub projection: ::prost::alloc::vec::Vec<u64>,
    #[prost(uint64, tag = "4")]
    pub global_limit: u64,
    #[prost(string, tag = "5")]
    pub state_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "6")]
    pub value: u32,
    #[prost(bool, tag = "7")]
    pub preempt_stage: bool,
}
