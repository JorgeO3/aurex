use bytes::{Bytes, BytesMut};

use crate::method::BasicPublish;
use crate::wire::properties::BasicProperties;
use crate::wire::ShortStr;

#[derive(Debug, Clone)]
pub struct PendingPublishContent {
    pub publish: BasicPublish,
    pub properties: BasicProperties,
    pub expected_body_size: u64,
    pub body: BytesMut,
}

#[derive(Debug, Clone)]
pub struct PublishMetadata {
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
    pub properties: BasicProperties,
    pub body: Bytes,
}
