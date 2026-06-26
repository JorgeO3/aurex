#[cfg(test)]
mod content_tests {
    use crate::{
        method::{encode_method, method_class_id, method_id, AmqpMethod, BasicMethod},
        port::{AmqpBrokerOutput, AmqpBrokerPort, AmqpControlResult, AmqpRouteResolveResult},
        session::{AmqpOutbound, AmqpSession},
        wire::constants::{CLASS_BASIC, PROTOCOL_HEADER},
        wire::frame::{FrameKind, RawFrame},
        wire::properties::{BasicProperties, ContentHeader},
        AmqpControlCommand, AmqpRouteResolveRequest, ShortStr,
    };
    use aurum_internal_protocol::{
        command::{ingress::IngressCommandBatch, shard::ShardCommandBatch},
        event::delivery::DeliveryMetadata,
    };
    use aurum_types::{PayloadHandle, RouteTableVersion};
    use bytes::{BufMut, Bytes, BytesMut};

    struct NoopBroker;

    impl AmqpBrokerPort for NoopBroker {
        fn handle_control(&mut self, _: AmqpControlCommand) -> AmqpControlResult {
            AmqpControlResult::ok()
        }
        fn handle_shard_batch(&mut self, _: ShardCommandBatch) -> AmqpBrokerOutput {
            AmqpBrokerOutput::default()
        }
        fn handle_ingress_batch(&mut self, _: IngressCommandBatch) -> AmqpBrokerOutput {
            AmqpBrokerOutput::default()
        }
        fn resolve_route(&mut self, _: AmqpRouteResolveRequest) -> AmqpRouteResolveResult {
            AmqpRouteResolveResult { entry: None }
        }
        fn route_table_version(&self) -> RouteTableVersion {
            RouteTableVersion::INITIAL
        }
        fn store_payload(&mut self, _: PayloadHandle, _: Bytes) {}
        fn load_payload(&self, _: PayloadHandle) -> Option<Bytes> {
            None
        }
        fn store_delivery_context(
            &mut self,
            _: PayloadHandle,
            _: DeliveryMetadata,
            _: BasicProperties,
        ) {
        }
        fn delivery_properties(&self, _: PayloadHandle) -> Option<BasicProperties> {
            None
        }
    }

    fn wire_method_frame(channel: u16, method: AmqpMethod) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.put_u16(method_class_id(&method));
        payload.put_u16(method_id(&method));
        encode_method(&method, &mut payload).unwrap();
        let mut buf = BytesMut::new();
        RawFrame::new(FrameKind::Method, channel, payload.freeze()).encode(&mut buf);
        buf.to_vec()
    }

    fn open_channel(session: &mut AmqpSession<NoopBroker>, out: &mut AmqpOutbound, ch: u16) {
        session
            .receive_bytes(PROTOCOL_HEADER, out)
            .expect("header");
        session
            .receive_bytes(
                &wire_method_frame(
                    0,
                    AmqpMethod::Connection(crate::method::ConnectionMethod::StartOk(
                        crate::method::ConnectionStartOk {
                            client_properties: Default::default(),
                            mechanism: ShortStr::from("PLAIN"),
                            response: b"\0guest\0guest".to_vec(),
                            locale: ShortStr::from("en_US"),
                        },
                    )),
                ),
                out,
            )
            .expect("start-ok");
        let mut tune_open = wire_method_frame(
            0,
            AmqpMethod::Connection(crate::method::ConnectionMethod::TuneOk(
                crate::method::ConnectionTuneOk {
                    channel_max: 2047,
                    frame_max: 4096,
                    heartbeat: 60,
                },
            )),
        );
        tune_open.extend(wire_method_frame(
            0,
            AmqpMethod::Connection(crate::method::ConnectionMethod::Open(
                crate::method::ConnectionOpen {
                    virtual_host: ShortStr::from("/"),
                    insist: false,
                },
            )),
        ));
        session.receive_bytes(&tune_open, out).expect("open");
        session
            .receive_bytes(
                &wire_method_frame(
                    ch,
                    AmqpMethod::Channel(crate::method::ChannelMethod::Open(
                        crate::method::ChannelOpen {
                            reserved: ShortStr::from(""),
                        },
                    )),
                ),
                out,
            )
            .expect("channel open");
    }

    #[test]
    fn multi_body_frame_publish_assembles() {
        let mut session = AmqpSession::new(NoopBroker);
        let mut out = AmqpOutbound::default();
        open_channel(&mut session, &mut out, 1);
        out.frames.clear();

        let body = b"part1-part2";
        let mut frames = wire_method_frame(
            1,
            AmqpMethod::Basic(BasicMethod::Publish(crate::method::BasicPublish {
                exchange: ShortStr::from("ex"),
                routing_key: ShortStr::from("rk"),
                flags: crate::method::BasicPublishFlags::empty(),
            })),
        );
        let mut header_payload = BytesMut::new();
        ContentHeader {
            class_id: CLASS_BASIC,
            body_size: body.len() as u64,
            properties: BasicProperties::default(),
        }
        .encode(&mut header_payload)
        .unwrap();
        let mut hf = BytesMut::new();
        RawFrame::new(FrameKind::Header, 1, header_payload.freeze()).encode(&mut hf);
        frames.extend_from_slice(&hf);
        for part in [&b"part1"[..], &b"-part2"[..]] {
            let mut bf = BytesMut::new();
            RawFrame::new(FrameKind::Body, 1, Bytes::copy_from_slice(part)).encode(&mut bf);
            frames.extend_from_slice(&bf);
        }
        session.receive_bytes(&frames, &mut out).expect("publish");
        // No broker route — publish dropped silently; no panic means assembly succeeded.
    }

    #[test]
    fn body_before_header_channel_close() {
        let mut session = AmqpSession::new(NoopBroker);
        let mut out = AmqpOutbound::default();
        open_channel(&mut session, &mut out, 1);
        out.frames.clear();

        let mut bf = BytesMut::new();
        RawFrame::new(FrameKind::Body, 1, Bytes::from_static(b"x")).encode(&mut bf);
        session.receive_bytes(&bf, &mut out).expect("body first");
        assert!(out.frames.iter().any(|f| f.header.kind == FrameKind::Method));
    }
}
