use crate::{HEAD_LENGHT, err::ErrorCode};
use tokio_util::{
    bytes::{self, Buf, BytesMut},
    codec::Decoder,
};

pub struct TransportCodec {
    max_frame_body_size: usize,
}

impl TransportCodec {
    pub fn new(max_frame_body_size: usize) -> Self {
        TransportCodec {
            max_frame_body_size,
        }
    }
}

impl Decoder for TransportCodec {
    type Item = (u8, u16, BytesMut);
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 未凑齐 Header 数据，继续拿数据
        if buf.len() < HEAD_LENGHT {
            return Ok(None);
        }

        // 注意：这只是用切片引用偷看，没有动底层真实指针
        let head = &buf[..HEAD_LENGHT];
        let version = head[0];
        let index: u16 = head[1..3].try_into().map(u16::from_be_bytes).unwrap();
        let body_length: u32 = head[3..HEAD_LENGHT]
            .try_into()
            .map(u32::from_be_bytes)
            .unwrap();

        if self.max_frame_body_size != 0 && body_length as usize > self.max_frame_body_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{}: Frame body length {} exceeds the maximum allowed {}",
                    ErrorCode::ExceedMaxMessageSize,
                    body_length,
                    self.max_frame_body_size
                ),
            ));
        }

        // 是否凑齐了完整的一帧数据（Header + Body）？如果没有，继续等数据
        if (buf.len() as u32) < (HEAD_LENGHT as u32 + body_length) {
            // 包不够长（遇到了典型的 TCP 半包）
            // 顺手优化：预先告诉底层池子，我还差多少，让池子一次性扩容，别挤牙膏
            buf.reserve(HEAD_LENGHT + (body_length as usize) - buf.len());
            return Ok(None);
        }

        // 已凑齐一帧数据，进行零拷贝切割
        let mut full_frame = buf.split_to(HEAD_LENGHT + (body_length as usize));

        // 头部已解析出来，直接丢弃
        full_frame.advance(HEAD_LENGHT);

        // 端给你的服务端 main loop！

        Ok(Some((version, index, full_frame)))
    }
}
