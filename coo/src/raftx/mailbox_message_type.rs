use raft::{
    eraftpb::ConfChange,
    prelude::{ConfChangeV2, Message},
};

pub enum MessageType {
    RaftPropose((Vec<u8>, Vec<u8>)),
    RaftConfChange(ConfChange),
    RaftConfChangeV2(ConfChangeV2),
    RaftMessage(Message),
}
